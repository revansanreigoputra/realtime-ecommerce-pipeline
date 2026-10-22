# Real-Time E-Commerce Data Pipeline

Pipeline data streaming untuk mensimulasikan transaksi e-commerce historis menggunakan Python, Apache Kafka, dan PostgreSQL.

## Arsitektur

Dataset Excel Online Retail II
→ Python Producer
→ Kafka topic `ecommerce-orders`
→ Python Consumer
→ Validasi event
→ PostgreSQL `raw_events` atau `dead_letter_events`

Dashboard disediakan melalui aplikasi Streamlit.

> Dataset ini bersifat historis dan dikirim secara bertahap untuk simulasi streaming. Pipeline ini tidak menerima transaksi e-commerce live.

## Teknologi

- Python 3.12
- Apache Kafka 4.0.0
- PostgreSQL 16
- Docker Compose
- pandas, openpyxl, psycopg, kafka-python
- pytest
- Streamlit

## Struktur Utama

- `src/pipeline/producer.py`: membaca dataset dan mengirim event ke Kafka.
- `src/pipeline/consumer.py`: menerima event, memvalidasi, dan menyimpan ke PostgreSQL.
- `src/pipeline/validation.py`: memeriksa struktur dan field transaksi.
- `src/pipeline/db.py`: mengatur koneksi PostgreSQL.
- `sql/01_create_tables.sql`: membuat tabel event mentah.
- `sql/02_analytics.sql`: membuat view analitik.
- `sql/03_create_dlq.sql`: membuat tabel Dead Letter Queue.
- `tests/`: unit test dan integration test.
- `dashboard/app.py`: aplikasi dashboard.
- `scripts/`: utilitas dataset dan simulasi streaming.

## Prasyarat

- Docker Engine dan Docker Compose
- Python 3.12
- Dataset Online Retail II yang tersedia secara lokal
- Linux atau WSL Ubuntu

## Konfigurasi

Buat `.env` hanya jika belum tersedia:

```bash
cp .env.example .env
```

Atur `POSTGRES_PASSWORD` pada `.env` dengan password lokal yang sesuai. Jangan commit atau membagikan `.env`.

Konfigurasi aplikasi secara default:

- Host: `localhost`
- Port: `5433`
- Database: `ecommerce_stream`
- User: `pipeline_user`

## Menjalankan Pipeline

### 1. Aktifkan virtual environment

```bash
source .venv/bin/activate
```

Jika environment belum tersedia:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Jalankan Kafka dan PostgreSQL

```bash
docker compose up -d
docker compose ps
```

Pastikan kedua layanan sehat sebelum melanjutkan.

### 3. Buat tabel database

Jalankan file SQL dari direktori proyek:

```bash
docker compose exec -T postgres psql \
  -U pipeline_user -d ecommerce_stream \
  < sql/01_create_tables.sql

docker compose exec -T postgres psql \
  -U pipeline_user -d ecommerce_stream \
  < sql/03_create_dlq.sql

docker compose exec -T postgres psql \
  -U pipeline_user -d ecommerce_stream \
  < sql/02_analytics.sql
```

### 4. Jalankan consumer

Buka Terminal 1:

```bash
PYTHONPATH=src python src/pipeline/consumer.py
```

Biarkan proses berjalan agar consumer menerima pesan Kafka.

### 5. Jalankan producer

Buka Terminal 2:

```bash
PYTHONPATH=src python src/pipeline/producer.py
```

Producer membaca dataset lokal dan mengirim event ke topic `ecommerce-orders`.

### 6. Periksa data

```bash
docker compose exec postgres psql \
  -U pipeline_user -d ecommerce_stream
```

Di prompt PostgreSQL:

```sql
SELECT COUNT(*) FROM raw_events;
SELECT COUNT(*) FROM dead_letter_events;
```

Keluar dengan `\q`.

## Menjalankan Dashboard

Periksa konfigurasi `dashboard/app.py` dan dependensi Streamlit sebelum menjalankan dashboard. Pastikan layanan dan database tersedia.

## Pengujian

Jalankan semua tes:

```bash
pytest -v
```

Unit test:

```bash
pytest -v tests/test_validation.py tests/test_consumer.py
```

Integration test:

```bash
pytest -v -m integration tests/integration/test_kafka_postgres.py
```

Integration test membutuhkan Kafka, PostgreSQL, serta consumer yang aktif.

## Hasil Verifikasi

Pada pemeriksaan sebelumnya:

- `raw_events`: 17 baris.
- `dead_letter_events`: 5 baris.
- Duplikasi fingerprint yang ditemukan: 0.
- Duplikasi posisi Kafka yang ditemukan: 0.
- Seluruh pengujian: 12 passed.

Hasil tersebut merupakan snapshot saat pemeriksaan, bukan jumlah tetap. Menjalankan producer atau integration test kembali dapat menambah data.

Pengujian terakhir menghasilkan `12 passed, 222 warnings`. Peringatan berasal dari API lama di dependensi Kafka dan penggunaan serializer pada integration test.

## Batasan

- Dataset transaksi bersifat historis.
- Kafka menggunakan satu broker untuk lingkungan pengembangan, bukan konfigurasi high availability.
- Integration test membutuhkan layanan lokal.
- Penyimpanan database menggunakan fingerprint event dan posisi pesan Kafka untuk mencegah duplikasi.

## Keamanan

- Jangan commit `.env`, password, token, atau rahasia lainnya.
- Dataset mentah dikecualikan melalui `.gitignore`.
- `.env.example` harus berisi nilai contoh, bukan password aktif.
