# PROMPT_LOG

## 2026-05-21 — Лабораторная работа 14, задание «Распределённый сборщик на Go»

### Промпт
> Распределённый сборщик на Go (координация через etcd)
> Реализовать систему, где несколько экземпляров Go-сборщика могут работать параллельно
> (каждый собирает свою часть данных). Использовать etcd для координации и
> распределения шардов/источников.

### Результат

Реализована полная система мониторинга качества воздуха с распределёнными Go-сборщиками.

#### Структура проекта
```
collector/                  — Go-сборщик
  cmd/collector/main.go     — точка входа, App, HTTP-сервер метрик
  internal/coordinator/     — etcd-координация
  internal/fetcher/         — клиент OpenAQ v3 API
  internal/publisher/       — NATS-публикатор (Arrow IPC)
  internal/schema/          — Arrow-схема измерений
  internal/metrics/         — сбор метрик производительности
  go.mod
analysis/                   — Python-анализ
  api/main.py               — FastAPI + подписка на NATS
  dashboard/app.py          — Streamlit-дашборд
  requirements.txt
k8s/                        — Kubernetes-манифесты (minikube/k3s)
  etcd.yaml, nats.yaml, collector.yaml, analysis.yaml
docker/                     — Dockerfile для Go и Python
docker-compose.yml          — локальный запуск
Makefile
```

#### Механизм координации (etcd)

1. **Регистрация**: каждый экземпляр создаёт `concurrency.Session` (TTL=15 с) и записывает
   ключ `/collectors/{id}` со своей метаинформацией, привязав его к lease.
2. **Выборы лидера**: `concurrency.NewElection` → `Campaign()`. Победитель становится
   «shard-coordinator».
3. **Распределение шардов**: лидер читает все ключи `/collectors/`, сортирует IDs и
   назначает страны round-robin: пишет `/assignments/{collector_id}/{country}` с привязкой
   к своему lease.
4. **Назначение**: каждый экземпляр смотрит `Watch(/assignments/{own_id}/)`. При `PUT`
   запускает горутину для сбора данных по стране, при `DELETE` — останавливает.
5. **Отказоустойчивость**: если лидер или воркер падает, TTL истекает, ключи удаляются
   автоматически → новый лидер переназначает шарды.

#### Конвейер данных

```
OpenAQ API → FetchMeasurements() → Arrow IPC (EncodeToArrow) → NATS air.quality.<CC>
                                                                        ↓
                                                          FastAPI (NATS subscribe)
                                                                        ↓
                                                          Streamlit dashboard
```

Формат сообщения: Arrow IPC stream с полями:
`location_id, location_name, country_code, city, latitude, longitude,
 parameter, value, unit, timestamp[us,UTC], collector_id`

#### Оценка производительности

Каждый цикл `fetchAndPublish` логирует структурированную запись:
```json
{
  "level": "info",
  "msg": "fetch_complete",
  "country": "US",
  "duration_ms": 4231.5,
  "records": 312,
  "bytes_published": 52480,
  "mem_alloc_bytes": 12582912
}
```

HTTP-эндпоинты каждого сборщика:
- `GET :8080/metrics` — JSON-массив всех FetchResult
- `GET :8080/shards`  — текущие назначенные шарды

Параметры замеряются:
- **Время**: `time.Since(start)` охватывает HTTP-запросы к OpenAQ + Arrow-кодирование + NATS-публикацию
- **Память**: `runtime.ReadMemStats().Alloc` после каждого цикла
- **Объём данных**: количество байт Arrow IPC, переданных в NATS

#### Запуск

```bash
# Локально (Docker Compose, 3 экземпляра сборщика):
make run-local

# Kubernetes (minikube):
make k8s-build-images
make k8s-deploy

# Дашборд:
# http://localhost:8501
```

Переменные среды:
| Переменная       | По умолчанию          | Описание                        |
|------------------|-----------------------|---------------------------------|
| ETCD_ENDPOINTS   | http://localhost:2379 | Список адресов etcd             |
| NATS_URL         | nats://localhost:4222 | URL NATS-сервера                |
| OPENAQ_API_KEY   | (пусто)               | Ключ API OpenAQ (необязательно) |
| FETCH_INTERVAL   | 5m                    | Интервал опроса на шард         |
| INSTANCE_ID      | (UUID)                | Идентификатор экземпляра        |
| METRICS_ADDR     | :8080                 | Адрес HTTP-сервера метрик       |
| WINDOW_DURATION  | 60s                   | Длина тамблинг-окна (по времени)|
| WINDOW_MAX_SIZE  | 500                   | Размер окна (по числу записей)  |
| PUBLISH_RAW      | true                  | Публиковать сырые данные в NATS |

---

## 2026-05-21 — Задание «Оконная агрегация в Go перед отправкой в Python»

### Промпт
> Оконная агрегация в Go перед отправкой в Python.
> Добавить в сборщик оконную агрегацию (tumbling window). Например: каждые N секунд
> или каждые M записей агрегировать данные на стороне Go и отправлять в Python не
> исходные записи, а уже агрегированные (суммы, средние и т.д.). Это снижает объём
> передаваемых данных.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `collector/internal/aggregator/window.go` | Tumbling window: накопление, триггеры (таймер / размер), функция aggregate() |
| `collector/internal/aggregator/encode.go` | Arrow IPC-схема + кодирование AggRecord |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `publisher.go` | добавлен `PublishAgg` → тема `air.agg.{CC}` |
| `metrics.go`   | добавлен `FlushResult`, `RecordFlush()`, `Summary` разделён на Fetches/Flushes |
| `main.go`      | `App.window`, горутина фlusher, `fetchAndPublish` → `window.Add()`, новый эндпоинт `/window` |
| `api/main.py`  | подписка `air.agg.*`, эндпоинты `/aggregated`, `/aggregated/stats` |
| `dashboard/app.py` | вкладки «Aggregated windows» и «Compression» |
| `k8s/collector.yaml` | WINDOW_DURATION, WINDOW_MAX_SIZE, PUBLISH_RAW |

#### Схема тамблинг-окна

```
fetchAndPublish(country)
  ├─ FetchMeasurements()  → []Measurement
  ├─ [если PUBLISH_RAW]   → Publish() → air.quality.{CC}   (Arrow IPC)
  └─ window.Add(...)
                            ↓ timer (60 s) или размер ≥ 500
                         flush()
                            ↓
                         aggregate(buf)
                            ↓  group by (country, parameter)
                            ↓  count, mean, min, max, std, location_count
                         PublishAgg() → air.agg.{CC}         (Arrow IPC)
```

#### Схема Arrow для агрегатов (`AggSchema`)
```
window_start   timestamp[us,UTC]
window_end     timestamp[us,UTC]
country_code   string
parameter      string
unit           string
count          int64      — кол-во сырых измерений в группе
mean_value     float64
min_value      float64
max_value      float64
std_value      float64    — стандартное отклонение (популяционное)
location_count int64      — кол-во уникальных станций
collector_id   string
```

#### Метрики производительности (window_flush)
```json
{
  "msg": "window_flush",
  "raw_records_in":   312,
  "agg_records_out":   14,
  "bytes_published": 2048,
  "compression_ratio": 22.3,
  "mem_alloc_bytes": 13107200
}
```
`compression_ratio = raw_count / agg_count` — показывает, во сколько раз сжаты данные.

HTTP: `GET :8080/metrics` возвращает `{"fetches":[...], "flushes":[...]}`.

#### Переменные среды (новые)
| Переменная      | По умолчанию | Описание                                    |
|-----------------|-------------|---------------------------------------------|
| WINDOW_DURATION | 60s         | Закрыть окно по истечении этого времени     |
| WINDOW_MAX_SIZE | 500         | Закрыть окно при накоплении N записей       |
| PUBLISH_RAW     | true        | Отправлять и сырые данные (доп. трафик)     |

---

## 2026-05-21 — Задание «Передача данных через Apache Arrow»

### Промпт
> Передача данных через Apache Arrow.
> Заменить передачу через JSON-файлы на передачу через Apache Arrow (Flight RPC или
> RecordBatch). Реализовать Go-сервер, отдающий данные в формате Arrow, и Python-клиент,
> принимающий их.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `collector/internal/flightserver/server.go` | Arrow Flight gRPC-сервер (Go) |
| `analysis/flight_client/client.py`          | Python Arrow Flight клиент |
| `analysis/flight_client/__init__.py`        | Пакет-инициализатор |
| `analysis/flight_demo.py`                   | Демо-скрипт с замерами |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `schema/air_quality.go` | Добавлена `BuildRecord()` — возвращает `arrow.Record` без сериализации |
| `aggregator/encode.go`  | Добавлена `BuildRecord()` для `AggRecord` |
| `main.go`               | Создаётся `FlightServer`, `FLIGHT_ADDR`, `FLIGHT_STORE_LEN`; `fetchAndPublish` → `AddRaw`; `publishAggregated` → `AddAgg` |
| `api/main.py`           | Эндпоинты `/flight/datasets`, `/flight/raw`, `/flight/aggregated`, `/flight/benchmark` |
| `dashboard/app.py`      | Вкладка "Arrow Flight (direct)" |
| `docker-compose.yml`    | Порты 5005/5006/5007, `FLIGHT_ENDPOINT` для analysis-api |
| `k8s/collector.yaml`    | `FLIGHT_ADDR`, `FLIGHT_STORE_LEN`, порт 5005 |

#### Архитектура передачи

```
Go collector
  ├─ fetchAndPublish()
  │   ├─ schema.BuildRecord()          ← один Arrow Record в памяти
  │   ├─ flightSrv.AddRaw(rec)         ← в ring-buffer (200 батчей)
  │   └─ publisher.Publish() → NATS    ← опционально
  └─ publishAggregated()
      ├─ aggregator.BuildRecord()
      ├─ flightSrv.AddAgg(rec)
      └─ publisher.PublishAgg() → NATS

Python client
  └─ AirQualityFlightClient("grpc://collector-1:5005")
      ├─ list_datasets()       → ListFlights RPC
      ├─ get_schema("raw")     → GetSchema RPC
      ├─ get_raw(country="US") → DoGet RPC → PyArrow Table → pandas
      ├─ get_aggregated()      → DoGet RPC → PyArrow Table → pandas
      └─ benchmark(runs=3)     → измеряет latency / throughput
```

#### Реализованные Flight RPC

| Метод Go (`FlightServer`) | Вызов Python |
|---------------------------|-------------|
| `ListFlights`             | `client.list_flights()` |
| `GetFlightInfo`           | `client.get_flight_info(descriptor)` |
| `GetSchema`               | `client.get_schema_info(descriptor)` |
| `DoGet`                   | `client.do_get(ticket)` |

#### Ring-buffer в Go

```
RecordStore (maxLen=200 батчей)
  add(rec)     → rec.Retain(); evict+Release при переполнении
  snapshot()   → каждый rec.Retain(); caller Release() после чтения
```
Zero-copy: Python читает данные через gRPC-поток без промежуточной сериализации в JSON.

#### Замеры производительности (benchmark endpoint)

```
GET /flight/benchmark?dataset=raw&runs=3
{
  "avg_rows": 312,
  "avg_bytes": 52480,
  "avg_latency_ms": 8.4,
  "throughput_rows_s": 37143,
  "throughput_mb_s": 6.248
}
```

#### Запуск демо

```bash
make run-local
# Дождаться первого цикла сбора, затем:
python analysis/flight_demo.py grpc://localhost:5005

# Или через API:
curl http://localhost:8000/flight/datasets
curl http://localhost:8000/flight/raw?country=US&limit=50
curl http://localhost:8000/flight/benchmark?dataset=raw&runs=3
```

#### Переменные среды (новые)
| Переменная       | По умолчанию   | Описание                                   |
|------------------|----------------|--------------------------------------------|
| FLIGHT_ADDR      | :5005          | Адрес gRPC-сервера Arrow Flight            |
| FLIGHT_STORE_LEN | 200            | Максимум батчей в ring-buffer (raw и agg)  |
| FLIGHT_ENDPOINT  | grpc://localhost:5005 | URL Flight-сервера для Python-клиента |
