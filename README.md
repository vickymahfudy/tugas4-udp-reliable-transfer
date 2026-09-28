# Tugas 4 Bagian 1.2: Pengiriman File Melalui UDP dengan Keandalan

Reliable file transfer di atas UDP: penomoran urutan paket, ACK & retransmisi
otomatis (selective repeat), kontrol kemacetan sederhana (AIMD), dan
pemisahan data & informasi kendali di level protokol aplikasi. Dilengkapi
baseline TCP sebagai pembanding kinerja.

Laporan analisis lengkap ada di `laporan_pdf_source.md` (source) dan
`Laporan_Analisis.pdf` (versi PDF).

## Struktur Repo

```
Kode/
  protocol.py             # Format paket UDP: header (seq_num, packet_type, checksum) + payload
  test_protocol.py         # Unit test protocol.py (encode/decode, deteksi korupsi)
  udp_receiver.py           # Receiver: terima META/DATA/FIN, kirim ACK, reassemble file
  udp_sender.py              # Sender: sliding window adaptif (cwnd AIMD), timeout+retransmit
  tcp_baseline_server.py      # Server TCP sederhana untuk baseline pembanding
  tcp_baseline_client.py       # Client TCP sederhana untuk baseline pembanding
  benchmark.py                  # Jalankan UDP vs TCP untuk beberapa ukuran file, catat waktu+throughput
  results/                       # Output benchmark.py (JSON + grafik)
screenshots/               # Bukti hasil pengujian (dipakai di laporan)
laporan_pdf_source.md      # Source laporan analisis (markdown, pandoc)
Laporan_Analisis.pdf       # Laporan analisis versi PDF
```

## Cara Menjalankan

Semua skrip menggunakan Python 3 standar (modul `socket`, `struct`, `zlib`),
tidak ada dependency eksternal untuk source utama. `benchmark.py` butuh
`matplotlib` untuk membuat grafik (`pip install matplotlib`).

### 1. Unit test protokol

```bash
cd Kode
python3 -m unittest test_protocol.py -v
```

### 2. Transfer file lewat UDP reliable

Jalankan receiver:

```bash
cd Kode
python3 udp_receiver.py --port 12000
```

Di terminal lain, jalankan sender:

```bash
# Transfer normal, tanpa simulasi loss
python3 udp_sender.py /path/ke/file --host 127.0.0.1 --port 12000

# Transfer dengan simulasi packet loss (retransmit + AIMD congestion control aktif)
python3 udp_sender.py /path/ke/file --host 127.0.0.1 --port 12000 --loss-rate 0.15
```

File hasil transfer disimpan di `Kode/received_files/`.

### 3. Baseline TCP

Jalankan server:

```bash
cd Kode
python3 tcp_baseline_server.py --port 12001
```

Di terminal lain, jalankan client:

```bash
python3 tcp_baseline_client.py /path/ke/file --host 127.0.0.1 --port 12001
```

File hasil transfer disimpan di `Kode/received_files_tcp/`.

### 4. Benchmark UDP vs TCP

Jalankan receiver UDP dan server TCP terlebih dahulu (langkah 2 dan 3 di
atas), lalu:

```bash
cd Kode
python3 benchmark.py --loss-rate 0.0
```

Hasil tersimpan di `Kode/results/benchmark_results.json` dan
`Kode/results/benchmark_chart.png`.

## Protokol Paket UDP

Setiap paket UDP dibungkus header biner 9 byte, diikuti payload:

```
seq_num (4B) | packet_type (1B) | checksum CRC32 (4B) | payload (0..1024B)
```

`packet_type` memisahkan informasi kendali (`META`, `ACK`, `FIN`, `FIN_ACK`)
dari data aplikasi (`DATA`) secara eksplisit di level protokol. Detail
lengkap desain, mekanisme reliability, congestion control, dan hasil
pengujian ada di `Laporan_Analisis.pdf`.
