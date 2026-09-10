# MigMaster Roll - FIXED

Perbaikan utama:
- Login semua akun dijalankan paralel, bukan satu per satu.
- Tetap memakai 1 WebSocket terpisah untuk setiap akun, sesuai dokumentasi Multi ID MigReborn.
- Browser hanya mengirim satu perintah START/STOP untuk ROLL; urutan ENTER/LEAVE dikerjakan server.
- Menghilangkan `/api/roll/send` per langkah yang membuat browser mudah mengalami `Failed to fetch`.
- SAVE memiliki nama file, default `troop1`, dan otomatis menambahkan `.json`.
- LOAD memulihkan akun, room, dan delay.
- Status WebSocket dan saldo ditampilkan.
- LOGOUT menutup seluruh koneksi.
- API command yang dipakai mengikuti dokumentasi `https://mig33.id/api.html`.

Jalankan:
`npm install`
`npm start`

Endpoint WebSocket:
`wss://developer.mig33.id/developer/ws`
