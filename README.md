# MigMaster Roll — Batch Login

Versi ini memproses jumlah User ID sesuai daftar yang dikirim tanpa batas angka buatan seperti 10/20/50.

## Login

Semua akun tetap dikelola dengan **1 WebSocket per akun**, tetapi pembukaan koneksi dilakukan bertahap:

- default 5 akun per batch
- jeda 500 ms antar-batch
- setelah berhasil login, seluruh WebSocket tetap aktif
- satu tombol LOGIN tetap mengendalikan seluruh daftar

Environment variable opsional:

- `LOGIN_BATCH_SIZE` — jumlah koneksi yang dibuka per batch (default 5, maksimum 5 pada versi aman ini)
- `LOGIN_BATCH_GAP_MS` — jeda antar-batch dalam milidetik (default 500)

Jika server/API memiliki batas koneksi aktif per IP/akun, batas tersebut tetap berlaku dan tidak dapat dihilangkan dari frontend.

## Start

```bash
npm install
npm start
```
