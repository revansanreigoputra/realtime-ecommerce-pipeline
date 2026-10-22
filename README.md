# Real-Time E-Commerce Data Pipeline

Pipeline data engineering untuk mensimulasikan aliran transaksi e-commerce historis menggunakan Python, Apache Kafka, dan PostgreSQL. Data yang telah diproses disajikan melalui dashboard interaktif menggunakan Streamlit.

Proyek ini menerapkan alur ingestion, validasi event, penyimpanan data mentah, penanganan event tidak valid melalui Dead Letter Queue (DLQ), dan penyajian metrik analitik.

> **Catatan:** Dataset yang digunakan adalah data historis UCI Online Retail II. Data dikirim secara bertahap untuk mensimulasikan streaming, bukan berasal dari transaksi e-commerce live.

## Arsitektur

```text
Dataset Excel Online Retail II
              |
              v
      Python Producer
              |
              v
     Kafka: ecommerce-orders
              |
              v
       Python Consumer
              |
              v
       Validasi Event
          /       \
         /         \
        v           v
   Event Valid   Event Tidak Valid
        |           |
        v           v
   PostgreSQL    Dead Letter Queue
   raw_events    dead_letter_events
        |
        v
   Analytics View
        |
        v
   Streamlit Dashboard
```

### Alur pemrosesan

1. Producer membaca baris transaksi dari file Excel.
2. Setiap baris dikemas menjadi event JSON.
3. Producer mengirim event ke topic Kafka `ecommerce-orders`.
4. Consumer menerima event dan memvalidasi struktur serta field wajib.
5. Event valid disimpan ke tabel `raw_events`.
6. Event tidak valid disimpan ke tabel `dead_letter_events` beserta alasan kegagalannya.
7. View analitik menyediakan ringkasan transaksi untuk dashboard Streamlit.

## Teknologi

| Teknologi          | Fungsi                                        |
| ------------------ | --------------------------------------------- |
| Python 3.12        | Bahasa pemrograman pipeline                   |
| Apache Kafka 4.0.0 | Message broker untuk simulasi streaming       |
| PostgreSQL 16      | Penyimpanan event dan hasil analitik          |
| Docker Compose     | Menjalankan Kafka dan PostgreSQL              |
| pandas             | Pengolahan data tabular                       |
| openpyxl           | Membaca file Excel                            |
| kafka-python       | Komunikasi producer dan consumer dengan Kafka |
| psycopg            | Koneksi Python ke PostgreSQL                  |
| Streamlit          | Dashboard interaktif                          |
| pytest             | Pengujian unit dan integrasi                  |

## Struktur Proyek

```text
realtime-ecommerce-pipeline/
├── dashboard/
│   └── app.py
├── data/
│   └── raw/
│       └── online_retail_II.xlsx
├── scripts/
│   ├── inspect_dataset.py
│   └── simulate_stream.py
├── sql/
│   ├── 01_create_tables.sql
│   ├── 02_analytics.sql
│   └── 03_create_dlq.sql
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── consumer.py
│       ├── db.py
│       ├── producer.py
│       ├── schemas.py
│       ├── test_dlq_producer.py
│       └── validation.py
├── tests/
│   ├── integration/
│   │   └── test_kafka_postgres.py
│   ├── test_consumer.py
│   └── test_validation.py
├── .env.example
├── .gitignore
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
└── README.md
```

### Fungsi komponen utama

- `src/pipeline/producer.py`: membaca dataset dan mengirim event ke Kafka.
- `src/pipeline/consumer.py`: menerima event, menjalankan validasi, dan menyimpan hasil ke PostgreSQL.
- `src/pipeline/validation.py`: memeriksa struktur event dan field wajib transaksi.
- `src/pipeline/db.py`: mengelola koneksi PostgreSQL.
- `src/pipeline/schemas.py`: mendefinisikan struktur data menggunakan type hints.
- `src/pipeline/test_dlq_producer.py`: utilitas untuk menghasilkan event tidak valid guna menguji DLQ.
- `scripts/inspect_dataset.py`: memeriksa karakteristik dataset sebelum diproses.
- `scripts/simulate_stream.py`: menjalankan producer untuk simulasi pengiriman event.
- `sql/01_create_tables.sql`: membuat tabel penyimpanan event.
- `sql/02_analytics.sql`: membuat view analitik.
- `sql/03_create_dlq.sql`: membuat tabel Dead Letter Queue.
- `dashboard/app.py`: aplikasi dashboard untuk memantau data pipeline.
- `tests/`: pengujian unit dan integrasi.

## Prasyarat

Pastikan perangkat sudah memiliki:

- Linux atau WSL Ubuntu.
- Python 3.12.
- Docker Engine dan Docker Compose.
- Dataset UCI Online Retail II dalam format Excel.
- Terminal yang dapat mengakses Docker dan direktori proyek.

## Instalasi dan Konfigurasi

### 1. Masuk ke direktori proyek

```bash
cd ~/projects/realtime-ecommerce-pipeline
```

### 2. Buat virtual environment

Jika `.venv` belum tersedia:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instal dependensi Python

```bash
pip install -r requirements.txt
```

### 4. Siapkan konfigurasi environment

Jika file `.env` belum tersedia:

```bash
cp .env.example .env
```

Sesuaikan konfigurasi lokal pada `.env` dengan `docker-compose.yml` dan `src/pipeline/db.py`.

Konfigurasi koneksi PostgreSQL yang digunakan oleh aplikasi:

| Parameter | Nilai                    |
| --------- | ------------------------ |
| Host      | `localhost`              |
| Port      | `5433`                   |
| Database  | `ecommerce_stream`       |
| User      | `pipeline_user`          |
| Password  | Sesuai konfigurasi lokal |

Jangan menyimpan password aktif, token, atau informasi rahasia di README maupun repository.

### 5. Siapkan dataset

Letakkan file Excel pada lokasi berikut:

```text
data/raw/online_retail_II.xlsx
```

Producer menggunakan sheet:

```text
Year 2009-2010
```

Nama dan lokasi file harus sesuai dengan konfigurasi pada producer.

## Menjalankan Pipeline

Pipeline dijalankan menggunakan beberapa terminal agar consumer dan dashboard tetap aktif selama producer mengirim event.

### 1. Jalankan Kafka dan PostgreSQL

Dari direktori utama proyek:

```bash
docker compose up -d
```

Periksa status container:

```bash
docker compose ps
```

Pastikan layanan Kafka dan PostgreSQL sudah berjalan sebelum melanjutkan.

### 2. Inisialisasi tabel dan view

Jalankan skrip SQL berikut dari direktori utama proyek:

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  < sql/01_create_tables.sql
```

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  < sql/03_create_dlq.sql
```

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  < sql/02_analytics.sql
```

Skrip tersebut menyiapkan tabel event, tabel DLQ, dan view analitik. Perintah ini tidak dimaksudkan untuk menghapus data yang sudah tersimpan; perilaku sebenarnya mengikuti isi skrip SQL.

### 3. Jalankan consumer

Buka **Terminal 1**:

```bash
cd ~/projects/realtime-ecommerce-pipeline
source .venv/bin/activate
PYTHONPATH=src python src/pipeline/consumer.py
```

Biarkan proses tetap berjalan. Consumer akan membaca event dari Kafka dan memprosesnya sesuai aturan validasi.

### 4. Jalankan producer

Buka **Terminal 2**:

```bash
cd ~/projects/realtime-ecommerce-pipeline
source .venv/bin/activate
MAX_EVENTS=10 python scripts/simulate_stream.py
```

Perintah tersebut membatasi pengiriman menjadi maksimal 10 baris untuk satu kali eksekusi.

Untuk pengujian dengan lima event:

```bash
MAX_EVENTS=5 python scripts/simulate_stream.py
```

Untuk menjalankan producer secara langsung:

```bash
PYTHONPATH=src python src/pipeline/producer.py
```

Producer mengirim data secara bertahap. Jumlah event yang benar-benar tersimpan dapat berbeda dari jumlah yang dikirim jika ada event tidak valid atau konflik data.

### 5. Periksa data PostgreSQL

Buka terminal lain dan hitung event yang tersimpan:

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  -c "SELECT COUNT(*) AS total_events FROM raw_events;"
```

Hitung event di DLQ:

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  -c "SELECT COUNT(*) AS dlq_events FROM dead_letter_events;"
```

Lihat beberapa event terbaru:

```bash
docker compose exec -T postgres psql \
  -U pipeline_user \
  -d ecommerce_stream \
  -c "SELECT event_id, event_type, source, kafka_topic, kafka_partition, kafka_offset FROM raw_events ORDER BY id DESC LIMIT 10;"
```

Nilai `total_events` dan `dlq_events` bergantung pada aktivitas producer dan pengujian yang sudah dijalankan.

## Menjalankan Dashboard

Buka **Terminal 3**:

```bash
cd ~/projects/realtime-ecommerce-pipeline
source .venv/bin/activate
PYTHONPATH=src python -m streamlit run dashboard/app.py
```

Setelah aplikasi berjalan, buka alamat berikut di browser:

```text
http://localhost:8501
```

Dashboard menampilkan ringkasan event tersimpan, Dead Letter Queue, serta metrik transaksi yang tersedia dari view analitik.

Pastikan Kafka dan PostgreSQL telah berjalan dan skema database sudah tersedia sebelum membuka dashboard.

## Pengujian

### 1. Jalankan seluruh pengujian

```bash
pytest -v
```

### 2. Jalankan unit test

```bash
pytest -v tests/test_validation.py tests/test_consumer.py
```

Unit test digunakan untuk memeriksa logika validasi dan pemrosesan event tanpa harus selalu menjalankan seluruh pipeline.

### 3. Jalankan integration test

```bash
pytest -v -m integration tests/integration/test_kafka_postgres.py
```

Integration test memerlukan layanan pendukung yang sesuai dengan konfigurasi pengujian, termasuk Kafka dan PostgreSQL.

Hasil pengujian perlu dicatat berdasarkan eksekusi terbaru. Jangan menganggap hasil pengujian sebelumnya sebagai hasil yang otomatis berlaku setelah perubahan kode.

## Hasil Verifikasi

Pada pengujian terakhir, pipeline berhasil melewati seluruh 12 automated tests.

| Pemeriksaan                      |     Hasil |
| -------------------------------- | --------: |
| Event di `raw_events`            |        98 |
| Fingerprint unik di `raw_events` |        98 |
| Event di `dead_letter_events`    |        21 |
| Posisi Kafka unik pada DLQ       |        21 |
| Automated tests                  | 12 passed |

Pemeriksaan menunjukkan bahwa jumlah event di `raw_events` sama dengan jumlah fingerprint unik. Pada DLQ, seluruh 21 event memiliki posisi Kafka yang berbeda.

Event tidak valid yang tercatat pada DLQ berasal dari `integration_test_dlq` sebanyak 19 event dan `dlq_test` sebanyak 2 event. Seluruhnya memiliki alasan kegagalan berupa field `data` dan `event_type` yang hilang.

Hasil tersebut merupakan snapshot pengujian lokal. Jumlah event dapat berubah ketika producer atau integration test dijalankan kembali. Gunakan query PostgreSQL untuk memeriksa kondisi terkini.

Pengujian menghasilkan sejumlah `DeprecationWarning` dari dependensi Kafka dan penggunaan `value_serializer` pada integration test. Peringatan tersebut tidak menggagalkan pengujian.

## Batasan

- Dataset yang digunakan bersifat historis, bukan transaksi e-commerce live.
- Pengiriman event secara bertahap digunakan untuk mensimulasikan streaming.
- Kafka menggunakan satu broker untuk lingkungan pengembangan, sehingga belum menyediakan high availability.
- Pengujian integrasi bergantung pada layanan dan konfigurasi lokal.
- Data yang tidak memenuhi aturan validasi diarahkan ke Dead Letter Queue.
- Mekanisme fingerprint dan posisi pesan Kafka digunakan untuk membantu mencegah penyimpanan event duplikat.

## Keamanan

- File `.env` tidak boleh di-commit ke repository.
- Gunakan `.env.example` untuk mendokumentasikan nama variabel konfigurasi tanpa menyertakan kredensial aktif.
- Jangan memasukkan password, token, atau informasi rahasia ke README.
- Dataset mentah dikecualikan dari Git melalui aturan `.gitignore`.
- Hindari membagikan data atau konfigurasi lokal yang bersifat sensitif.

## Pengembangan Selanjutnya

Beberapa pengembangan yang dapat dipertimbangkan:

- Menambahkan logging terstruktur dan metrik operasional pipeline.
- Memperbaiki peringatan deprecation dari library Kafka.
- Menambahkan pemantauan consumer lag dan throughput.
- Memperluas pengujian untuk skenario event duplikat dan payload tidak valid.
- Menambahkan konfigurasi deployment dan pemantauan yang lebih sesuai untuk lingkungan produksi.
