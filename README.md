# PLN Analytics Platform

Platform analitik berbasis web untuk membantu pemantauan data pelanggan dan hasil pemeriksaan PLN.

## Modul

- **Executive Dashboard** — ringkasan KPI dan kondisi data.
- **DLPD Monitoring** — pemantauan pelanggan, unit, detail, riwayat pemeriksaan, dan export data.
- **Suspect Analytics** — analisis kategori suspect, frekuensi, lokasi, dan detail hasil pemeriksaan.
- **Data Management** — preview dan pemeriksaan dataset hasil ETL.

## Teknologi

| Bagian | Teknologi |
|---|---|
| Backend | Python, FastAPI |
| Database/Analitik | DuckDB |
| Frontend | React, Vite, TypeScript |
| ETL | Python |
| Data | Excel, Parquet |
| Deployment | Render, Cloudflare Pages |

Struktur backend dipisahkan menjadi domain, application, infrastructure, dan interface/API agar masing-masing bagian memiliki tanggung jawab yang jelas.

---

## 1. Prasyarat

Install:

| Tool | Versi | Cek dengan |
|---|---|---|
| Python | 3.11+ | `python --version` |
| Node.js | 20+ | `node --version` |
| Git | bebas | `git --version` |

Untuk deployment online, repository GitHub, Render, dan Cloudflare Pages dapat digunakan sesuai kebutuhan.

---

## 2. Menjalankan di Lokal

### Backend

Dari root project:

```bash
cd backend
python -m venv .venv
```

Windows:

```powershell
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Install dependency:

```bash
pip install -r requirements.txt
```

Buat file environment:

```powershell
Copy-Item .env.example .env
```

Jalankan:

```bash
uvicorn app.main:app --reload
```

Backend:

```text
http://localhost:8000
```

Dokumentasi API:

```text
http://localhost:8000/api/docs
```

### Frontend

Buka terminal baru:

```bash
cd frontend
npm install
```

Buat file environment:

```powershell
Copy-Item .env.example .env
```

Jalankan:

```bash
npm run dev
```

Frontend:

```text
http://localhost:5173
```

---

## 3. Data Contoh dan ETL

Untuk pengujian tanpa data operasional, gunakan generator data contoh.

Dari root project:

```bash
python -m venv .venv
```

Aktifkan environment lalu install dependency:

```bash
pip install pandas openpyxl pyarrow numpy
```

Generate data contoh:

```bash
python -m etl.generate_sample_data --output-dir data/raw --months 202606 202607
```

Jalankan ETL:

```bash
python -m etl.run_etl --input-dir data/raw --output-dir data/processed
```

Hasil proses ditempatkan di `data/processed/`.

Tahapan ETL mencakup discovery, pembacaan sumber, klasifikasi, transformasi, validasi, penggabungan data per bulan, deduplikasi, dan penulisan data hasil.

---

## 4. Menggunakan Data Excel

File Excel dapat ditempatkan pada `data/raw/` sesuai struktur sumber.

Contoh sumber:

- DLPD Pascabayar — sheet `main`
- DLPD Prabayar — sheet `Sheet1`
- Pengecekan — sheet `DATA`
- File ANEV sesuai periode yang akan diproses

Jalankan:

```bash
python -m etl.run_etl --input-dir data/raw --output-dir data/processed
```

Jenis file dikenali berdasarkan struktur kolom yang tersedia. Daftar schema dapat dilihat pada:

```text
etl/config/schema_registry.py
```

Untuk data operasional, gunakan penyimpanan yang sesuai dengan kebijakan akses data dan jangan memasukkan data pelanggan ke repository publik.

---

## 5. Modul Aplikasi

### Executive Dashboard

Menampilkan ringkasan KPI seperti:

- Total Pelanggan
- Suspect
- Normal
- Temuan
- Sisa Pemeriksaan
- Progress
- Hit Rate

Tersedia grafik berdasarkan periode yang dipilih.

### DLPD Monitoring

Fitur utama:

- Filter berdasarkan UNITUPI, UNITAP, dan UNITUP.
- Pencarian berdasarkan IDPEL atau nama.
- Ringkasan per ULP.
- Daftar pelanggan.
- Detail pelanggan.
- Riwayat pemeriksaan.
- Export CSV/Excel.

### Suspect Analytics

**Main**

Menampilkan ringkasan suspect berdasarkan pelanggan dan frekuensi.

**Summary**

Menampilkan rekap per lokasi pelanggan dan kategori anomali.

**Detail**

Menampilkan detail pembacaan untuk lokasi yang dipilih, termasuk grafik tren tegangan dan arus.

### Data Management

Digunakan untuk melihat preview dataset, filter, dan informasi data yang tersedia.

---

## 6. Struktur Proyek

```text
pln-analytics-proto/
├── backend/
│   ├── app/
│   │   ├── domain/
│   │   ├── application/
│   │   ├── infrastructure/
│   │   └── interface/
│   ├── data/
│   ├── tests/
│   └── requirements.txt
├── etl/
│   ├── config/
│   ├── pipeline/
│   ├── notebooks/
│   └── run_etl.py
├── frontend/
│   └── src/
├── tests/
├── deployment/
├── documentation/
├── data/
├── RUN_LOCAL.bat
└── STOP_LOCAL.bat
```

---

## 7. Deployment

### Backend — Render

1. Push repository ke GitHub.
2. Buat service baru di Render.
3. Hubungkan repository.
4. Gunakan konfigurasi pada `deployment/render.yaml`.
5. Atur environment variable backend.
6. Setelah deployment selesai, cek:

```text
https://<backend-url>/api/health
```

### Frontend — Cloudflare Pages

Gunakan konfigurasi:

```text
Root directory: frontend
Build command: npm run build
Build output directory: dist
```

Environment variable:

```text
VITE_API_BASE_URL=https://<backend-url>/api/v1
```

Backend perlu mengizinkan URL frontend melalui konfigurasi `CORS_ORIGINS`.

---

## 8. Update Data Bulanan

Untuk menambahkan periode baru:

1. Tambahkan file Excel periode tersebut.
2. Jalankan ETL.
3. Pastikan hasil proses masuk ke `data/processed/`.
4. Restart backend atau lakukan deployment ulang sesuai konfigurasi.

Selama struktur sumber masih sesuai dengan schema ETL, penambahan periode tidak memerlukan perubahan kode.

---

## 9. Keamanan

Sebelum digunakan pada lingkungan produksi:

- Ganti password akun default.
- Gunakan `JWT_SECRET_KEY` yang kuat melalui environment variable.
- Periksa konfigurasi `CORS_ORIGINS`.
- Jangan commit file `.env`.
- Jangan commit data pelanggan atau hasil ETL yang bersifat sensitif.
- Batasi akses repository dan storage sesuai kebutuhan.

---

## 10. Troubleshooting

| Masalah | Kemungkinan penyebab | Solusi |
|---|---|---|
| `ModuleNotFoundError: No module named 'fastapi'` | Dependency backend belum terpasang | `pip install -r backend/requirements.txt` |
| Frontend gagal dijalankan | Dependency Node belum terpasang | `cd frontend` lalu `npm install` |
| Login gagal | Konfigurasi user/environment belum sesuai | Periksa konfigurasi backend dan `USERS_FILE` |
| Data tidak muncul | ETL belum dijalankan atau path salah | Jalankan ETL dan periksa `DATA_PROCESSED_DIR` |
| CORS error | Origin frontend belum diizinkan | Periksa `CORS_ORIGINS` |
| Request pertama lambat setelah deployment | Service sedang startup | Tunggu proses startup selesai lalu coba kembali |

---

## 11. Pengujian

Repository menyediakan test untuk backend, ETL, repository, dan use-case.

Backend:

```bash
cd backend
pytest
```

Frontend:

```bash
cd frontend
npm install
npm run build
```

---

## 12. Dokumentasi

Dokumentasi arsitektur:

```text
documentation/ARCHITECTURE.md
```

Dokumentasi deployment:

```text
docs/github-actions-etl.md
```

Panduan instalasi:

```text
FINAL_INSTALL_GUIDE.md
```
