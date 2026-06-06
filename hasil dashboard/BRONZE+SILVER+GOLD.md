# 📂 Panduan Upload Data Tier (Bronze / Silver / Gold)

Dokumen ini menjelaskan cara menambahkan data ke folder **Bronze**, **Silver**, dan **Gold** melalui Google Drive.

---

## 🗂️ Struktur Folder

| Tier | Deskripsi | Link Folder |
|------|-----------|-------------|
| 🥉 **Bronze** | Data mentah / raw, belum diproses | [Buka Folder Bronze](https://drive.google.com/drive/folders/1q0G1ilWHSDhEG34NxQLz3dEsjfnPjkiC?usp=drive_link) |
| 🥈 **Silver** | Data bersih / cleaned, siap dianalisa | [Buka Folder Silver](https://drive.google.com/drive/folders/1bR4WZyEthNfmfx4m4v5sfFdLWljUWLF9?usp=drive_link) |
| 🥇 **Gold** | Data final / agregat, siap untuk dashboard | [Buka Folder Gold](https://drive.google.com/drive/folders/1WS4bYXTFKdgrF9RHEAPsl7ebpEIZTV45?usp=drive_link) |

---

## Langkah-langkah Upload Data

### 1. Buka folder yang sesuai
Klik salah satu link di tabel di atas sesuai level data yang akan diunggah.

### 2. Upload file
- Klik tombol **+ Baru** di pojok kiri atas Google Drive
- Pilih **Upload file**
- Atau **drag & drop** file langsung ke dalam folder

### 3. Periksa nama file
Gunakan format nama yang konsisten agar mudah ditelusuri:

```
<tier>_<nama_dataset>_<YYYYMMDD>.<ekstensi>
```

**Contoh:**
```
bronze_penjualan_20250601.csv
silver_penjualan_20250601.csv
gold_penjualan_20250601.csv
```

### 4. Konfirmasi upload selesai
Pastikan file sudah muncul di folder Drive dan tidak ada tanda error (ikon merah/segitiga kuning).

---

## Konvensi Penamaan File

| Tier | Format | Contoh |
|------|--------|--------|
| Bronze | `bronze_<dataset>_<YYYYMMDD>.<ext>` | `bronze_transaksi_20250601.csv` |
| Silver | `silver_<dataset>_<YYYYMMDD>.<ext>` | `silver_transaksi_20250601.csv` |
| Gold | `gold_<dataset>_<YYYYMMDD>.<ext>` | `gold_transaksi_20250601.csv` |

---

## 🔄 Alur Data (Pipeline)

```
[Sumber Data]
      │
      ▼
  🥉 Bronze  ← Upload data mentah / raw di sini
      │         (CSV, Excel, JSON, dll. tanpa perubahan)
      │
      ▼
  🥈 Silver  ← Upload setelah data dibersihkan
      │         (hapus duplikat, normalisasi, validasi)
      │
      ▼
  🥇 Gold    ← Upload data final
               (agregat, siap untuk laporan / dashboard)
```

---

## Catatan Penting

- Pastikan kamu memiliki akses **Editor** ke folder Drive agar bisa mengupload file.
- Jika belum punya akses, minta admin untuk membagikan folder dengan izin yang sesuai.
- **Jangan** upload data Bronze langsung ke folder Silver atau Gold.
- Untuk file berukuran besar (>100 MB), gunakan fitur **Upload folder** atau kompresi ZIP terlebih dahulu.
- Setiap perubahan data harus didokumentasikan dalam file log atau komentar pada Google Drive.

---

## 🔗 Link Folder

- [📁 Folder Bronze](https://drive.google.com/drive/folders/1q0G1ilWHSDhEG34NxQLz3dEsjfnPjkiC?usp=drive_link)
- [📁 Folder Silver](https://drive.google.com/drive/folders/1bR4WZyEthNfmfx4m4v5sfFdLWljUWLF9?usp=drive_link)
- [📁 Folder Gold](https://drive.google.com/drive/folders/1WS4bYXTFKdgrF9RHEAPsl7ebpEIZTV45?usp=drive_link)
