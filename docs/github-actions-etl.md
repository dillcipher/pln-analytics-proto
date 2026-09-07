# Menjalankan proses merge ETL lewat GitHub Actions

Kenapa: host API produksi (FastAPI Cloud Hobby, gratis) cuma dikasih 0.1-0.5
vCPU dan RAM 512MB, dan containernya berkali-kali restart/reset di tengah
proses merge ANEV/DLPD/PENGECEKAN yang berat -- setiap kali itu terjadi,
progress yang lagi jalan hilang diam-diam. GitHub Actions runner dapat
sekitar 7GB RAM dan bisa jalan sampai 6 jam tanpa keganggu, jauh lebih cocok
buat proses berat sekali-jalan seperti ini.

Yang dipindah: cuma langkah merge-nya. Backend FastAPI tetap seperti biasa
untuk login, dashboard, dan baca data -- itu ringan dan tidak pernah jadi
sumber masalah.

## Setup sekali saja (checklist)

1. **Bikin repo private** (Settings -> General -> Danger Zone -> Change
   repository visibility). Ini lapisan keamanan utama: kalau repo public,
   log Actions kelihatan oleh siapa saja; kalau private, cuma orang yang
   punya akses ke repo. Gratis untuk akun personal (dapat 2.000 menit/bulan
   Actions di repo private, jauh lebih dari cukup).

2. **Tambahkan secrets** di Settings -> Secrets and variables -> Actions ->
   New repository secret. Nilainya SAMA PERSIS dengan yang sudah dipakai di
   environment variables FastAPI Cloud sekarang -- tinggal disalin dari
   sana, jangan generate baru:
   - `S3_ENDPOINT`
   - `S3_REGION`
   - `S3_ACCESS_KEY_ID`
   - `S3_SECRET_ACCESS_KEY`
   - `S3_BUCKET`

   Opsional (cuma perlu kalau ada file mentah yang belum sempat ke-cache
   penuh ke S3 dan perlu diunduh ulang dari Drive):
   - `GOOGLE_SERVICE_ACCOUNT_JSON` atau `GOOGLE_SERVICE_ACCOUNT_JSON_B64`

## Cara menjalankan

**Otomatis dari backend (2026-09-03, baru):** sekarang backend sendiri BISA
melakukan langkah "push file JSON" di bawah secara otomatis, persis begitu
sebuah job upload Google Drive selesai sync dan statusnya jadi READY FOR
ETL -- tidak perlu lagi developer push manual tiap kali ada upload baru.
Ini aktif kalau env var `GITHUB_ETL_TOKEN` sudah diisi di FastAPI Cloud;
kalau kosong, backend tetap jalan seperti sebelumnya (proses ETL langsung
di host API, yang berisiko kena reset container). Lihat bagian "Setup
trigger otomatis dari backend" di bawah untuk cara mengisinya.

Cakupan saat ini: HANYA job yang sumbernya Google Drive (`storage ==
"google_drive"`, ini pola upload yang dipakai atasan sejauh ini) yang
di-offload otomatis -- `scripts/run_etl_from_github_actions.py` baru bisa
merestore file mentah dari cache durable S3 untuk job jenis ini. Upload
lewat form upload browser biasa (bukan Drive) untuk saat ini masih diproses
langsung di host API seperti sebelumnya.

**Otomatis lewat push manual (cara lama, masih berfungsi):** tambah/ubah
file `.github/etl-jobs/<JOB_ID>.json` lalu push ke `main`. Nama file
(tanpa `.json`) itu `job_id`-nya. Workflow langsung jalan sendiri, tidak
perlu klik apa-apa di GitHub. Ini yang dipakai dari sandbox Claude
sepanjang sesi debugging 2026-09-02/03 (workflow_dispatch tidak bisa
dipanggil dari sana), dan tetap berguna sebagai cara manual kalau
diperlukan.

**Manual lewat GitHub UI:** tab Actions -> pilih workflow "ETL Merge
(offloaded from API host)" -> Run workflow -> isi `job_id`.

## Setup trigger otomatis dari backend (opsional, tapi disarankan)

Ini yang membuat "atasan upload file -> otomatis ke dashboard" beneran
mulus tanpa campur tangan manual tiap kali. Sekali setup saja.

1. **Bikin GitHub Personal Access Token (PAT) yang scope-nya SEKECIL
   MUNGKIN** -- pakai tipe *fine-grained* (bukan classic), supaya bisa
   dibatasi ke SATU repo ini saja:
   - GitHub -> foto profil -> Settings -> Developer settings ->
     Personal access tokens -> Fine-grained tokens -> Generate new token.
   - Repository access: **Only select repositories** -> pilih
     `dillcipher/pln-analytics-platform` saja.
   - Permissions: **Contents** -> **Read and write**. Semua permission
     lain biarkan default (No access). Token ini TIDAK butuh akses ke
     Actions, Issues, Pull requests, atau apapun selain Contents repo ini.
   - Expiration: pilih sesuai kenyamanan (bisa "No expiration" kalau mau
     bebas urusan, atau expiry lalu diperpanjang manual tiap beberapa
     bulan -- ini trade-off keamanan vs kenyamanan, terserah yang punya
     akun).
   - Generate, lalu **salin token-nya sekali itu saja** (GitHub tidak
     menampilkannya lagi setelah halaman ini ditutup).

2. **Masukkan token itu ke FastAPI Cloud**, BUKAN ke mana pun di repo ini
   atau dikirim ke siapa pun lewat chat: dashboard FastAPI Cloud -> app
   `pln-analytics-platform` -> Settings/Environment variables -> tambah
   variable baru:
   - Nama: `GITHUB_ETL_TOKEN`
   - Value: token yang baru dibuat di langkah 1.
   
   Env var lain (`GITHUB_ETL_REPO`, `GITHUB_ETL_BRANCH`) opsional -- default-nya
   sudah benar (`dillcipher/pln-analytics-platform` dan `main`), cuma perlu
   diisi kalau suatu saat repo dipindah/di-rename atau branch default beda.

3. **Redeploy backend** (env var baru biasanya perlu restart/redeploy biar
   kebaca) -- push commit apa saja ke `main`, atau pakai tombol
   redeploy manual di dashboard FastAPI Cloud kalau ada.

4. **Verifikasi**: upload/sync satu file test lewat Google Drive seperti
   biasa. Begitu statusnya READY FOR ETL, cek tab Actions di GitHub --
   seharusnya ada run baru yang mulai sendiri (dipicu oleh push commit
   otomatis dari backend, pesan commit-nya `chore: auto-trigger ETL for
   <job_id>`), tanpa ada yang push manual.

Kalau `GITHUB_ETL_TOKEN` belum diisi, atau panggilan ke GitHub API-nya
gagal karena sebab apapun (token salah, repo/branch salah, GitHub API lagi
down, dll.), backend otomatis fallback ke perilaku lama (proses ETL
langsung di host API) -- tidak ada yang rusak kalau setup ini belum/tidak
dilakukan.

## Apa yang terjadi setelah selesai

Tidak perlu langkah tambahan. Hasil merge (parquet + warehouse.duckdb)
otomatis ke-upload ke S3 lewat kode yang sama seperti biasa (lihat
`app/infrastructure/storage/processed_storage.py`), dan backend FastAPI
otomatis narik data terbaru itu setiap kali dia buka koneksi warehouse baru
(lihat `Warehouse.connect()` -> `ensure_hydrated()`) -- yang, mengingat
container-nya sering restart sendiri, biasanya kejadian dalam beberapa
menit tanpa perlu dipicu manual. Kalau mau langsung tanpa nunggu restart,
panggil `POST /api/v1/warehouse/refresh` (perlu login).

## Yang TIDAK diubah

Logic merge/transform-nya sendiri tidak diubah sama sekali -- script runner
(`backend/scripts/run_etl_from_github_actions.py`) hanya memanggil kode yang
sudah ada dan sudah diuji di produksi (`app.main` untuk semua patch,
`run_etl_serialized` untuk proses mergenya, `_sync_drive_job` untuk restore
file mentah dari cache durable). Tidak ada logic baru yang berisiko.
