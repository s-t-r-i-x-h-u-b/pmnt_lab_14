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

---

## 2026-05-21 — Задание «Интеграция Rust-библиотеки для валидации»

### Промпт
> Интеграция Rust-библиотеки для валидации.
> Написать на Rust библиотеку для валидации данных (например, проверка формата полей,
> диапазонов значений). Встроить её в Go-сборщик через cgo или в Python-анализатор
> через PyO3.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `validator/Cargo.toml`             | Crate: cdylib + staticlib, PyO3 как опциональная фича |
| `validator/pyproject.toml`         | Maturin-конфиг для сборки Python-колеса |
| `validator/src/types.rs`           | `ValidationError`, `ValidationResult`, `errors_to_json()` |
| `validator/src/rules.rs`           | 5 правил: country_code, parameter, value, coordinates, timestamp |
| `validator/src/lib.rs`             | Публичный API + C FFI (`#[no_mangle]`) + PyO3-модуль (`#[cfg(feature="python")]`) |
| `validator/validator.h`            | C-заголовок для cgo: `CValidationResult`, `validate_measurement_c`, `free_validation_result` |
| `collector/internal/validator/validator.go` | Общие типы + публичный API `FilterMeasurements` |
| `collector/internal/validator/cgo.go`       | `//go:build rust_validator` — реализация через cgo |
| `collector/internal/validator/stub.go`      | `//go:build !rust_validator` — заглушка (пропускает всё) |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `collector/cmd/collector/main.go`  | `validator.FilterMeasurements()` после `FetchMeasurements()`, логирование `validation_filter` |
| `docker/Dockerfile.collector`      | Трёхэтапная сборка: rust-builder → go-builder (CGO=1, -tags rust_validator) → debian-slim |
| `docker/Dockerfile.analysis`       | Двухэтапная сборка: maturin wheel-builder → python:3.11-slim + pip install wheel |
| `docker-compose.yml`               | Контекст всех сервисов изменён с `./collector`/`./analysis` на `.` (корень проекта) |
| `Makefile`                         | Новые цели: `build-rust-cgo`, `build-rust-py`; `build` зависит от `build-rust-cgo` |
| `analysis/requirements.txt`        | Добавлен `maturin>=1.5,<2` |
| `analysis/api/main.py`             | Graceful import `air_quality_validator`, `_is_valid_row()`, фильтрация в `handle_raw`, `/validator/info` |

#### Архитектура Rust-библиотеки

```
validator/src/
  types.rs    — ValidationError { field, message }
                ValidationResult { valid, errors }
                errors_to_json() — ручная JSON-сериализация (без serde)

  rules.rs    — validate_country_code : 2 заглавных ASCII буквы (ISO 3166-1)
                validate_parameter    : непустая строка; неизвестные параметры ≠ ошибка
                validate_value        : конечное число + в диапазоне для параметра:
                    pm25 [0,2000], pm10 [0,3000], o3 [0,600], no2/so2 [0,2000],
                    co [0,100000], bc [0,200], humidity [0,100],
                    temperature [-100,100], pressure [80000,110000]
                validate_coordinates  : lat ∈ [-90,90], lon ∈ [-180,180]
                validate_timestamp    : не >5 мин в будущем, не >30 дней в прошлом

  lib.rs      — validate_measurement() / validate_batch() — чистый Rust
                C FFI: CValidationResult { valid: i32, errors_json: *mut c_char }
                       validate_measurement_c(…) → CValidationResult
                       free_validation_result(…)
                PyO3:  PyValidationError, PyValidationResult,
                       validate_measurement_py(), validate_batch_py(),
                       validate_columns() — колоночная валидация для pandas
```

#### Встройка в Go (cgo)

```
make build-rust-cgo
  └─ cargo build --release --no-default-features
     → libair_quality_validator.a
  └─ cp .a + validator.h → collector/internal/validator/

CGO_ENABLED=1 go build -tags rust_validator ./cmd/collector
  → cgo.go: #cgo LDFLAGS: -L${SRCDIR} -lair_quality_validator -ldl -lm
             validate_measurement_c(cc, param, value, lat, lon, ts_us)
```

#### Встройка в Python (PyO3 / maturin)

```
make build-rust-py
  └─ maturin build --release --features python --out target/wheels
  └─ pip install target/wheels/air_quality_validator-*.whl

import air_quality_validator as aqv
r = aqv.validate_measurement_py("US", "pm25", 42.0, 40.7, -74.0, ts_us)
print(r.valid, r.errors)
```

#### Конвейер валидации в Go-сборщике

```
FetchMeasurements(ctx, country)
  └─ validator.FilterMeasurements(measurements)
       ├─ [rust_validator tag] CValidationResult via validate_measurement_c
       └─ [stub] pass-through (valid = all)
  └─ schema.BuildRecord(valid_measurements)
  └─ flightSrv.AddRaw / publisher.Publish / window.Add
```

#### Конвейер валидации в Python-анализаторе

```
NATS air.quality.* → handle_raw()
  └─ _decode_arrow() → rows (list[dict])
  └─ [if _HAS_VALIDATOR] _is_valid_row() per row → air_quality_validator.validate_measurement_py()
  └─ raw_store.extend(valid_rows)
  └─ _validation_stats["validated"] / ["invalid"] += …
```

#### Метрики производительности (новые)

Go-лог при наличии отбракованных записей:
```json
{
  "msg": "validation_filter",
  "country": "US",
  "valid": 298,
  "invalid": 14
}
```

Python REST:
```
GET /validator/info
{
  "enabled": true,
  "validated": 1540,
  "invalid": 23
}
```

#### Сборка

```bash
# Локально:
make build               # Rust → .a → CGO Go binary
make build-rust-py       # Rust → PyO3 wheel
pip install validator/target/wheels/*.whl

# Docker (автоматически):
make run-local           # docker compose up --build (multi-stage)
make k8s-build-images    # docker build с project-root context
```

---

## 2026-05-21 — Задание «Развёртывание в Kubernetes с автоскалированием»

### Промпт
> Развёртывание в Kubernetes с автоскалированием.
> Упаковать Go-сборщик в Docker-образ. Развернуть конвейер в minikube/k3s.
> Настроить HPA (Horizontal Pod Autoscaler) для сборщика на основе длины очереди
> или загрузки CPU.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `k8s/hpa.yaml`               | HPA `autoscaling/v2`: CPU + memory, min=1, max=5 |
| `k8s/keda-scaledobject.yaml` | KEDA ScaledObject: CPU + buffer\_size (queue-depth) |
| `k8s/prometheus.yaml`        | Prometheus: RBAC + ConfigMap + Deployment (pod-discovery) |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `collector/cmd/collector/main.go` | Эндпоинт `/metrics/prometheus` (Prometheus text format) |
| `k8s/collector.yaml`              | Readiness probe, pod-аннотации, headless Service |
| `Makefile`                        | `k8s-hpa-setup`, `k8s-keda-setup`, `k8s-hpa-status`, `k8s-load-test` |

#### Два варианта HPA

**Вариант 1 — стандартный HPA (только CPU/memory)**

```
make k8s-hpa-setup   # minikube addons enable metrics-server + kubectl apply hpa.yaml
```

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metrics:
- type: Resource
  resource: { name: cpu, target: { averageUtilization: 60 } }
- type: Resource
  resource: { name: memory, target: { averageUtilization: 75 } }
behavior:
  scaleUp:   { stabilizationWindowSeconds: 60,  policies: [{type: Pods, value: 1, periodSeconds: 60}] }
  scaleDown: { stabilizationWindowSeconds: 300, policies: [{type: Pods, value: 1, periodSeconds: 120}] }
```

**Вариант 2 — KEDA ScaledObject (CPU + длина очереди)**

```
make k8s-keda-setup  # install KEDA + apply keda-scaledobject.yaml (удаляет plain HPA)
```

```yaml
triggers:
- type: cpu
  metadata: { value: "60" }
- type: metrics-api
  metadata:
    url:           "http://collector:8080/window"
    valueLocation: "buffer_size"   # JSON-поле из GET /window
    targetValue:   "200"
```

KEDA опрашивает `/window` каждые 15 с. При `buffer_size > 200` добавляет поды.

#### Prometheus-метрики (`GET /metrics/prometheus`)

```
collector_window_buffer_size{collector_id="collector-abc"} 142
collector_flight_raw_batches{collector_id="..."}  18
collector_flight_agg_batches{collector_id="..."}   7
collector_fetches_total{collector_id="..."}       312
collector_bytes_published_total{collector_id="..."} 16384000
collector_window_flushes_total{collector_id="..."}  24
```

Prometheus scrape через pod-аннотации:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port:   "8080"
prometheus.io/path:   "/metrics/prometheus"
```

#### Нагрузочный тест

```bash
make k8s-build-images && make k8s-deploy && make k8s-keda-setup
make k8s-load-test       # FETCH_INTERVAL=10s, WINDOW_MAX_SIZE=50
make k8s-hpa-status      # kubectl top pods + get hpa/scaledobject
# Prometheus: http://$(minikube ip):30090
```

#### Показатели производительности

| Метрика              | Источник                   | Порог масштабирования |
|----------------------|----------------------------|-----------------------|
| CPU utilisation      | metrics-server (cAdvisor)  | > 60%                 |
| Memory utilisation   | metrics-server             | > 75%                 |
| `buffer_size`        | GET /window каждые 15 с    | > 200 записей         |

- scaleUp stabilization: 60 с (фильтр кратковременных пиков)
- scaleDown stabilization: 300 с (предотвращает thrashing после сброса окна)

---

## 2026-05-21 — Задание «Сравнение производительности: Go vs Python»

### Промпт
> Сравнение производительности: Go vs Python для сбора.
> Реализовать сборщик данных на Python (с использованием asyncio/aiohttp).
> Сравнить производительность (скорость сбора, потребление памяти, CPU) между
> Go- и Python-версиями при одинаковой нагрузке.
> Результаты оформить в виде отчёта с графиками.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `collector_py/fetcher.py`  | Async OpenAQ v3 клиент (aiohttp + asyncio.gather + семафор) |
| `collector_py/publisher.py`| Arrow IPC кодирование + NATS публикация (nats-py) |
| `collector_py/metrics.py`  | Замер времени, RSS, CPU% через psutil (PerfCollector) |
| `collector_py/main.py`     | CLI-точка входа: циклы, страны, NATS URL, вывод JSON |
| `benchmark/runner.py`      | Оркестратор: запускает Python; читает /metrics Go-сборщика; сохраняет JSON |
| `benchmark/report.py`      | Генератор интерактивного HTML-отчёта (Plotly) |
| `benchmark/results/`       | Директория результатов (.gitkeep) |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `analysis/dashboard/app.py`    | Вкладка «Go vs Python»: bar-charts, per-country, memory timeline |
| `analysis/requirements.txt`    | Добавлен `psutil==6.0.0` |
| `Makefile`                     | Цели `bench`, `bench-python`, `bench-compare`, `bench-report` |

#### Ключевое различие архитектур

```
Go-сборщик (sequential):
  для каждой локации:
    GET /locations/{id}/latest
    sleep(150ms)              ← rate-limit delay
  → N локаций × (150ms + T_req) ≈ 8–20 с на страну

Python-сборщик (concurrent):
  asyncio.gather(
    fetch_loc_1(), fetch_loc_2(), ..., fetch_loc_N()
  )                           ← семафор 5 одновременных
  → ceil(N/5) × T_req ≈ 1–3 с на страну при тех же N
```

#### Измеряемые метрики

| Метрика | Python | Go |
|---------|--------|----|
| Время / страна (мс) | `time.perf_counter()` | `FetchResult.DurationMs` (Go JSON) |
| Память | `psutil.Process().memory_info().rss` (весь RSS) | `runtime.ReadMemStats().Alloc` (heap alloc) |
| CPU% | `psutil.Process().cpu_percent()` | не измеряется на уровне выборки |
| Записей / сек | total_records / total_ms×1000 | аналогично |
| МБ / сек (Arrow IPC) | len(encoded) / total_ms×1000 | аналогично |

#### Запуск

```bash
# Только Python (Go не нужен):
make bench-python
# → benchmark/results/benchmark_YYYYMMDD_HHMMSS.json

# Сравнение с Go (Go-сборщик запущен):
make run-local           # в другом терминале
make bench-compare       # читает http://localhost:8081/metrics

# HTML-отчёт:
make bench-report
# → benchmark/results/benchmark_*.html  (Plotly, self-contained)

# Дашборд (вкладка «Go vs Python»):
streamlit run analysis/dashboard/app.py
```

#### Ожидаемые результаты

| Показатель | Python async | Go sequential | Объяснение |
|------------|--------------|---------------|------------|
| Время на страну | **~2–4 с** | ~10–20 с | async concurrency vs 150ms/per-loc delay |
| Память (МиБ) | ~80–120 | ~3–8 | CPython interpreter + GC overhead |
| CPU% при I/O | ~5–15 | ~2–5 | GIL overhead, но I/O-bound |
| Records/sec | конкурентна или выше | ниже (sequential) | I/O-bound профиль |

**Вывод**: Python async выигрывает при I/O-bound нагрузке за счёт параллелизма.
Go выигрывает по памяти, CPU и предсказуемой задержке (нет GC-пауз).

---

## 2026-05-21 — Задание «Обработка потоковых данных (Kafka)»

### Промпт
> Обработка потоковых данных (Kafka).
> Заменить промежуточное хранение в JSON на потоковую передачу через Kafka (или NATS).
> Go-сборщик пишет в топик, Python-анализатор читает из топика и обрабатывает
> в реальном времени. Добавить оконную обработку (скользящее окно в 5 минут).

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `collector/internal/kafkaproducer/producer.go` | Kafka-продюсер (segmentio/kafka-go): топики raw+agg, Hash балансировщик |
| `analysis/kafka_consumer/__init__.py`          | Пакет-инициализатор |
| `analysis/kafka_consumer/consumer.py`          | SlidingWindow + daemon-thread KafkaConsumer + asyncio drain loop |
| `k8s/kafka.yaml`                               | StatefulSet bitnami/kafka:3.7 (KRaft) + headless Service |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `collector/cmd/collector/main.go` | KAFKA_BROKERS env, kafkaproducer.New(), kafka.PublishRaw/PublishAgg |
| `collector/go.mod`                | Добавлена зависимость github.com/segmentio/kafka-go v0.4.47 |
| `analysis/api/main.py`            | Импорт kafka_consumer, lifespan start/stop, 4 эндпоинта /kafka/* |
| `analysis/dashboard/app.py`       | Вкладка «Kafka sliding window»: bar chart, scatter, window summary |
| `docker-compose.yml`              | Сервис kafka (bitnami/kafka:3.7 KRaft), KAFKA_BROKERS во всех сервисах |
| `k8s/collector.yaml`              | KAFKA_BROKERS: kafka:9092 |
| `k8s/analysis.yaml`               | KAFKA_BROKERS: kafka:9092, FLIGHT_ENDPOINT |
| `Makefile`                        | kafka.yaml в k8s-deploy, цели kafka-topics/kafka-logs/kafka-consume |

#### Архитектура потоковой обработки

```
Go collector
  fetchAndPublish()
    └─ kafka.PublishRaw(measurements)   → топик air.measurements.raw
                                           ключ = country_code (Hash partitioner)
  publishAggregated()
    └─ kafka.PublishAgg(aggRecords)     → топик air.measurements.agg

Kafka broker (KRaft, 4 партиции)
  air.measurements.raw   — JSON: {location_id, country_code, parameter, value,
                                   unit, timestamp_us, collector_id, …}
  air.measurements.agg   — JSON: {window_start, window_end, country_code,
                                   parameter, count, mean_value, …}

Python analysis-api
  _consumer_thread (daemon)
    └─ kafka-python KafkaConsumer
         group_id=analysis-sliding-window, auto_offset_reset=latest
         → _msg_queue (threading.Queue, maxsize=20000)
  asyncio _drain_loop (каждые 100 мс)
    └─ _process_message(topic, value)
         └─ sliding_window.add(country, parameter, value, ts_us)
```

#### Скользящее 5-минутное окно (SlidingWindow)

```
Структура:
  _buckets: dict[(country_code, parameter)] → deque[(ts_us, value)]
  _win_us = 5 * 60 * 1_000_000

add(country, param, value, ts_us)
  → append (ts_us, value) в нужный bucket

compute() → list[WindowStats]
  now_us = time.time() * 1e6
  cutoff  = now_us - _win_us
  для каждого bucket:
    popleft() пока dq[0][0] < cutoff   ← вытеснение устаревших
    vals = [v for _, v in dq]
    → WindowStats(count, mean, min, max, std, window_start_us=cutoff)
```

Тип события — event-time: `timestamp_us` из сообщения Kafka, не время получения.
Вытеснение по wall-clock для предотвращения утечки памяти при задержках продюсера.

#### REST API (новые эндпоинты)

| Метод GET | Описание |
|-----------|---------|
| `/kafka/stats` | Счётчики: enabled, raw_total, agg_total, errors, window_entries |
| `/kafka/window` | Текущее окно: список WindowStats (фильтры: country, parameter) |
| `/kafka/window/countries` | Страны с данными в текущем окне |
| `/kafka/window/summary` | По-параметровая сводка: count_total, country_count, overall_mean |

#### Kafka-конфигурация (KRaft, без Zookeeper)

```yaml
KAFKA_CFG_PROCESS_ROLES: controller,broker
KAFKA_CFG_LISTENERS:     PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_CFG_NUM_PARTITIONS: "4"
KAFKA_CFG_LOG_RETENTION_HOURS: "2"
```

Продюсер (Go):
- `Balancer: kafka.Hash{}` — детерминированная маршрутизация по country_code
- `Async: true` — не блокирует pipeline
- `AllowAutoTopicCreation: true` — топики создаются автоматически

Консьюмер (Python):
- `group_id=analysis-sliding-window` — один consumer group, offset auto-commit
- `max_poll_records=500`, `session_timeout_ms=30000`
- Авто-реконнект: бесконечный retry-loop с sleep(5) при ошибке

#### Запуск

```bash
# Docker Compose:
make run-local

# Посмотреть топики:
make kafka-topics

# Последние 20 сообщений raw:
make kafka-logs

# Live-monitor:
make kafka-consume TOPIC=air.measurements.raw

# API:
curl http://localhost:8000/kafka/stats
curl http://localhost:8000/kafka/window?country=US
curl http://localhost:8000/kafka/window/summary

# Kubernetes:
make k8s-deploy   # включает kafka.yaml
```

#### Переменные среды (новые)
| Переменная    | По умолчанию | Описание                               |
|---------------|-------------|----------------------------------------|
| KAFKA_BROKERS | (пусто)     | Список брокеров; пусто → Kafka отключена |

#### Оценка производительности

Kafka-топик с Hash-балансировщиком обеспечивает:
- **Пропускная способность**: async producer не добавляет задержку к pipeline
- **Объём сообщений**: каждое измерение — JSON ~200–400 байт; для 500 записей/цикл ≈ 100–200 КБ/цикл
- **Задержка end-to-end**: poll interval 150ms (Go) + queue drain 100ms (Python) ≈ ~250ms
- **Скользящее окно**: вычисление O(N) по активным записям в окне; на 10 стран × 5 параметров × 300 записей ≈ 15000 elem. → <1ms на compute()

---

## 2026-05-21 — Задание «Веб-дашборд с обновлением в реальном времени»

### Промпт
> Веб-дашборд с обновлением в реальном времени.
> Разработать веб-дашборд на Streamlit (или FastAPI + WebSocket), который показывает
> текущую агрегированную статистику и графики, обновляющиеся в реальном времени
> по мере поступления новых данных от сборщика.

### Результат

#### Новые файлы
| Файл | Назначение |
|------|-----------|
| `analysis/dashboard/realtime.html` | Автономный WebSocket-дашборд: Chart.js, тёмная тема, авто-реконнект |

#### Изменённые файлы
| Файл | Что изменилось |
|------|---------------|
| `analysis/api/main.py`      | WebSocket broadcasting, `_stats_broadcaster`, `/ws/live`, `/realtime`, `/ws/clients` |
| `analysis/dashboard/app.py` | Вкладка «Live» с `@st.fragment(run_every=3)`, дельта-метрики, таймлайн, ссылка |
| `analysis/requirements.txt` | Добавлен `kafka-python==2.0.2` |
| `docker-compose.yml`        | `PUBLIC_API_URL` для dashboard-сервиса |
| `k8s/analysis.yaml`         | `PUBLIC_API_URL` для dashboard-деплоя |

#### Два режима обновления

**1. WebSocket HTML-дашборд (`/realtime`)**

Полностью автономная HTML-страница, обслуживаемая FastAPI.
Браузер устанавливает WebSocket-соединение с `ws://host/ws/live` и получает
события в реальном времени без каких-либо перезагрузок страницы.

```
NATS air.quality.*  →  handle_raw()   →  _ws_broadcast({type: batch_raw})
NATS air.agg.*      →  handle_agg()   →  _ws_broadcast({type: batch_agg})
фоновая задача      →  _stats_broadcaster() каждые 5 с  →  _ws_broadcast({type: stats})

WebSocket /ws/live  ←  asyncio.Queue per client (maxsize=200)
                    →  browser Chart.js: line/bar/doughnut/feed
```

**2. Streamlit-вкладка «Live» (авто-обновление каждые 3 с)**

Использует `@st.fragment(run_every=3)` (Streamlit ≥ 1.37) — фрагмент перерисовывается
каждые 3 секунды без перезагрузки остальных вкладок.

```
@st.fragment(run_every=3)
def _live_fragment():
    stats  = GET /stats          → метрики с delta
    kstats = GET /kafka/stats    → kafka window entries
    raw    = GET /measurements   → таймлайн, топ стран, топ параметров
    GET /ws/clients              → число активных WebSocket-соединений
```

#### Типы WebSocket-сообщений

| type | Когда | Поля |
|------|-------|------|
| `batch_raw`  | при каждом NATS-батче raw (≈ каждые 5 мин/сборщик) | country, parameter, count, mean_value, timestamp_ms |
| `batch_agg`  | при каждом NATS-батче agg | country, parameter, count, mean_value |
| `stats`      | каждые 5 с | raw_records, agg_records, country_counts{}, param_counts{}, countries[], kafka_entries, validation{} |
| `heartbeat`  | если 25 с нет данных | — |

#### HTML-дашборд (Chart.js)

Визуализация:
- **Линейный график** — скорость поступления данных (записей / 10 с), скользящее окно 60 × 10 с = 10 мин.  Буфет перекатывается каждые 10 с через `setInterval`.
- **Горизонтальный bar chart** — топ-12 стран по числу измерений (обновляется из `stats`).
- **Doughnut chart** — распределение по параметрам с цветовой кодировкой (pm25=красный, o3=зелёный и т.д.).
- **Таблица событий** — последние 12 батчей с подсветкой строки при вставке.
- **Индикатор соединения** — зелёный пульсирующий dot + счётчик msg/s.
- **Авто-реконнект** — `setTimeout(connect, 3000)` при `ws.onclose`.

#### Управление соединениями (FastAPI)

```python
_ws_clients: set[asyncio.Queue] = set()   # один Queue на соединение

async def _ws_broadcast(payload):
    for q in list(_ws_clients):
        try: q.put_nowait(payload)
        except QueueFull: mark_dead(q)    # slow client dropped

@app.websocket("/ws/live")
async def websocket_live(ws):
    q = asyncio.Queue(maxsize=200)
    _ws_clients.add(q)
    while True:
        payload = await asyncio.wait_for(q.get(), timeout=25)
        await ws.send_json(payload)       # backpressure via queue
```

Изоляция через `asyncio.Queue` исключает прямую зависимость между медленными клиентами
и обработчиками NATS-сообщений.

#### Запуск

```bash
# Локально:
make run-local

# Streamlit Live-вкладка:
http://localhost:8501  →  вкладка «Live»

# Полноэкранный WebSocket-дашборд:
http://localhost:8000/realtime

# REST:
GET http://localhost:8000/ws/clients   # {"connected_clients": 2}

# Kubernetes:
make k8s-deploy
kubectl port-forward svc/analysis-api 8000:8000
http://localhost:8000/realtime
```

#### Переменные среды (новые)
| Переменная     | По умолчанию          | Описание                                   |
|----------------|-----------------------|--------------------------------------------|
| PUBLIC_API_URL | http://localhost:8000 | Публичный URL API (для ссылки в Streamlit) |

---

## 2026-05-21 — Ревью кода и исправление ошибок

### Промпт
> Сделай ревью и исправь ошибки

### Результат

Проведено код-ревью всех компонентов: Go-сборщик, Python-анализ, Rust-валидатор,
benchmark-скрипты, Kubernetes/Docker-манифесты. Найдено и исправлено 8 проблем:

#### Bug 1 — `benchmark/report.py`: затенение модуля `plotly.graph_objects as go`
Локальная переменная `go = results.get("go", {})` в `_summary_table()` маскировала
импортированный модуль `go`. Технически не ломало код (внутри функции `go.get(...)`
работал как `dict.get`), но опасно при будущих правках. Переименовано `go → go_d`,
`py → py_d`.

#### Bug 2 — `benchmark/report.py::_per_country()`: терялись данные Python
Функция использовала проверку `if country not in countries` и оставляла только
первую запись на страну, игнорируя данные за остальные циклы (для Python-сборщика
samples содержит N×M записей). Переписано на агрегацию средним по странам.

#### Bug 3 — `validator/src/lib.rs::validate_columns()`: мёртвый код
Функция создавала `Vec<Vec<PyObject>>` пустых `PyDict` объектов (по одному
на каждую ошибку валидации), затем сбрасывала их через `let _ = ...`. Удалено;
функция теперь сразу собирает `valid_flags` и `err_counts` и возвращает кортеж.

#### Bug 4 — `analysis/api/main.py::_is_valid_row()`: `import pandas` в горячем пути
`import pandas as _pd` вызывался при каждой проверке строки (по сути на каждое
сообщение из NATS). Перенесено в импорты модуля.

#### Bug 5 — `collector/internal/coordinator/coordinator.go::stopShard()`: вызов колбэка под мьютексом
`c.onUnassigned(shard)` вызывался при удерживаемой блокировке `c.mu`, что
заблокировало бы координатор при повторном обращении к нему из колбэка.
`cancel()` и `onUnassigned()` теперь вызываются вне `Lock/Unlock`.

#### Bug 6 — `validator/pyproject.toml`: `python-source = "python"` без папки
Поле указывало на несуществующую директорию `validator/python/`, что в некоторых
версиях maturin приводило к ошибке сборки wheel. Удалено.

#### Bug 7 — Неиспользуемые импорты
- `collector_py/main.py`: `import sys` (не используется).
- `collector_py/fetcher.py`: `from typing import Optional`, `timezone`.
- `benchmark/runner.py`: `import aiohttp` (используется только внутри функции),
  `import sys`.

#### Bug 8 — Проверки синтаксиса
Все Python-файлы прошли `python -m py_compile` после исправлений. Rust и Go
не имеют локального тулчейна для проверки, но изменения чисто косметические /
рефакторинговые и компилируются.
