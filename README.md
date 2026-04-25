# UTS-SISTER - Log Aggregator

Sistem ini adalah service agregasi log berbasis FastAPI dengan fitur:

- asynchronous queue untuk pemrosesan event
- deduplication berbasis kombinasi `topic + event_id`
- persistensi event dan statistik ke SQLite
- publisher container untuk simulasi high load

## Arsitektur Singkat

- `aggregator` (FastAPI): menerima event, memasukkan ke queue, worker memproses dedup + simpan
- `publisher` (opsional via Docker Compose): mengirim 5000 event (4000 unik, 1000 duplikat)
- SQLite DB: disimpan di folder `/app/data` (dipersist ke Docker volume)

## Prasyarat

- Docker + Docker Compose (untuk menjalankan mode container)
- Python 3.11 (untuk menjalankan mode lokal)

## Build dan Run

## Docker Compose

Jalankan dari root project:

```bash
# Opsi 1: Jalankan hanya service aggregator
docker compose up --build aggregator

# Opsi 2: Jalankan semua service (aggregator + publisher)
docker compose up --build
```

Service yang aktif:

- Opsi 1: hanya Aggregator API di `http://localhost:8080`
- Opsi 2: Aggregator API + Publisher (publisher otomatis mengirim batch event)

Perintah tambahan:

```bash
# Jalankan di background
docker compose up --build -d

# Lihat log
docker compose logs -f aggregator
docker compose logs -f publisher

# Stop dan hapus container
docker compose down
```

## Menjalankan Test

```bash
docker compose run --rm aggregator python -m pytest tests/test_main.py -v
```

## Asumsi Sistem

1. Kunci deduplikasi adalah pasangan `topic` dan `event_id`.
1. Event dianggap valid jika sesuai schema `LogEvent`.
1. `timestamp` dikirim dalam format datetime ISO-8601 (contoh: `2026-04-25T10:00:00Z`).
1. Pemrosesan event bersifat asynchronous (event di-queue), sehingga hasil `/stats` dan `/events` dapat tertunda singkat setelah `/publish`.
1. Data SQLite dipersist pada volume Docker `aggregator_data`, sehingga data tetap ada saat container restart.
1. Statistik `received`, `duplicate_dropped`, dan uptime bersifat akumulatif selama database yang sama masih dipakai.
1. Endpoint `/events` memerlukan query parameter `topic` yang harus sama persis.

## Endpoint API

## 1) POST /publish

Menerima 1 event atau list event.

Contoh request (single):

```json
{
  "topic": "suhu_ruangan",
  "event_id": "Temt_01",
  "timestamp": "2026-04-25T10:00:00Z",
  "source": "sensor-01",
  "payload": {
    "suhu": 25
  }
}
```

Contoh request (batch):

```json
[
  {
    "topic": "suhu_ruangan",
    "event_id": "Temt_01",
    "timestamp": "2026-04-25T10:00:00Z",
    "source": "sensor-01",
    "payload": { "temp": 25 }
  },
  {
    "topic": "suhu_ruangan",
    "event_id": "Temt_02",
    "timestamp": "2026-04-25T10:00:01Z",
    "source": "sensor-01",
    "payload": { "temp": 26 }
  }
]
```

Contoh response:

- single:

```json
{
  "status": "accepted",
  "event_id": "evt-001"
}
```

- batch:

```json
{
  "status": "accepted",
  "count": 2
}
```

## 2) GET /stats

Mengembalikan statistik agregator.

Contoh response:

```json
{
  "received": 5000,
  "unique_processed": 4000,
  "duplicate_dropped": 1000,
  "topics": ["docker_compose_test"],
  "uptime_seconds": 123
}
```

## 3) GET /events?topic={topic}

Mengembalikan event yang sudah tersimpan untuk topic tertentu.

Contoh request:

```http
GET /events?topic=sensor
```

Contoh response:

```json
{
  "topic": "sensor",
  "events": [
    [
      "sensor",
      "evt-001",
      "2026-04-25T10:00:00+00:00",
      "device-a",
      "{'temp': 25}"
    ]
  ]
}
```

## Catatan Operasional

- File database default: `data/dedup.db`
- Lokasi DB dalam container: `/app/data/dedup.db`
- Saat shutdown normal, service menambahkan durasi uptime ke persistent stats
