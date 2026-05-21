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
