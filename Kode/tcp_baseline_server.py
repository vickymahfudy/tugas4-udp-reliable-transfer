"""
Server TCP sederhana untuk baseline pembanding kinerja terhadap
udp_receiver.py. Bukan bagian dari requirement 1.2 (yang wajib UDP),
tetapi diperlukan brief: "Bandingkan kinerjanya dengan transfer file
melalui TCP".

Protokol sangat sederhana (length-prefixed framing), cukup untuk
transfer satu file per koneksi:

    [4 byte panjang nama file N1 (big-endian)] [N1 byte nama file UTF-8]
    [8 byte panjang isi file N2 (big-endian)]  [N2 byte isi file]

TCP menjamin byte stream reliable & in-order, sehingga tidak perlu
sequence number/ACK manual seperti di UDP: itulah justru poin
pembanding kinerja & kompleksitas yang dibahas di laporan.
"""

import argparse
import os
import socket
import struct
import sys

DEFAULT_PORT = 12001
DEFAULT_OUTPUT_DIR = 'received_files_tcp'
HEADER_NAME_LEN = 4   # 4 byte untuk panjang nama file
HEADER_SIZE_LEN = 8   # 8 byte untuk panjang isi file (cukup untuk file besar)


def recv_exact(conn: socket.socket, n: int) -> bytes:
    """Baca tepat n byte dari socket TCP, menangani short read berulang."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = conn.recv(min(remaining, 65536))
        if not chunk:
            raise ConnectionError('Koneksi TCP putus sebelum data lengkap diterima.')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def run_server(host: str, port: int, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f'TCP baseline server siap, listening di {host}:{port}...')

    while True:
        conn, addr = server_socket.accept()
        try:
            _handle_client(conn, addr, output_dir)
        except (ConnectionError, OSError) as exc:
            print(f'[{addr}] Error saat menerima file: {exc}')
        finally:
            conn.close()


def _handle_client(conn: socket.socket, addr, output_dir: str) -> None:
    name_len = struct.unpack('!I', recv_exact(conn, HEADER_NAME_LEN))[0]
    filename = recv_exact(conn, name_len).decode('utf-8')
    filesize = struct.unpack('!Q', recv_exact(conn, HEADER_SIZE_LEN))[0]

    print(f'[{addr}] Menerima file {filename!r} ({filesize} byte)...')
    data = recv_exact(conn, filesize)

    safe_name = os.path.basename(filename)
    out_path = os.path.join(output_dir, safe_name)
    with open(out_path, 'wb') as f:
        f.write(data)

    written_size = os.path.getsize(out_path)
    status = 'OK' if written_size == filesize else 'UKURAN TIDAK COCOK'
    print(f'[{addr}] File selesai ditulis: {out_path} ({written_size} byte) [{status}]')


def main():
    parser = argparse.ArgumentParser(description='TCP baseline file transfer server')
    parser.add_argument('--host', default='', help='Bind address (default: semua interface)')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    run_server(args.host, args.port, args.output_dir)


if __name__ == '__main__':
    sys.exit(main())
