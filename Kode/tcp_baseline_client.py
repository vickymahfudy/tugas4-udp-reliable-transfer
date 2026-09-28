"""
Client TCP sederhana untuk baseline pembanding kinerja terhadap
udp_sender.py. Mengirim satu file lewat satu koneksi TCP menggunakan
protokol length-prefixed yang sama seperti didokumentasikan di
tcp_baseline_server.py.
"""

import argparse
import os
import socket
import struct
import sys
import time

DEFAULT_PORT = 12001


def send_file(host: str, port: int, filepath: str) -> dict:
    with open(filepath, 'rb') as f:
        data = f.read()

    filename = os.path.basename(filepath)
    filesize = len(data)
    name_bytes = filename.encode('utf-8')

    start_time = time.monotonic()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    try:
        sock.sendall(struct.pack('!I', len(name_bytes)))
        sock.sendall(name_bytes)
        sock.sendall(struct.pack('!Q', filesize))
        sock.sendall(data)
    finally:
        sock.close()

    elapsed = time.monotonic() - start_time
    return {
        'filename': filename,
        'filesize': filesize,
        'elapsed_seconds': elapsed,
        'throughput_bytes_per_sec': filesize / elapsed if elapsed > 0 else float('inf'),
    }


def main():
    parser = argparse.ArgumentParser(description='TCP baseline file transfer client')
    parser.add_argument('filepath', help='Path file yang akan dikirim')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    stats = send_file(args.host, args.port, args.filepath)

    print('\n=== Statistik Transfer TCP ===')
    print(f"File: {stats['filename']} ({stats['filesize']} byte)")
    print(f"Waktu: {stats['elapsed_seconds']:.4f} s")
    print(f"Throughput: {stats['throughput_bytes_per_sec'] / 1024:.2f} KB/s")


if __name__ == '__main__':
    sys.exit(main())
