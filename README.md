# MigMaster Roll

Project baru berbasis official MigReborn Developer WebSocket API.

- Satu WebSocket per User ID yang diload.
- Save/Load User ID + password memakai localStorage browser.
- Login membuat array WebSocket sesuai jumlah User ID.
- Enter Room dan Leave Room.
- CommandBox `ROLL`: WebSocket 1 ENTER -> LEAVE, lanjut WebSocket 2, dst, kemudian kembali ke WebSocket 1 tanpa batas sampai STOP ROLL.
- Delay dapat diatur dalam milidetik.
- WebSocket mengirim ping setiap 40 detik setelah `session.ready`.

API: https://mig33.id/api.html
Endpoint: wss://developer.mig33.id/developer/ws
