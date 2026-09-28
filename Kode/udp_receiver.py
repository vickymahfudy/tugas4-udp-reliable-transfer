"""
Receiver (server) untuk reliable file transfer di atas UDP.

Alur:
  1. Terima paket META (nama file, ukuran, total jumlah paket DATA).
  2. Terima paket DATA satu per satu (boleh datang out-of-order/duplikat
     karena UDP tidak menjamin ordering maupun exactly-once delivery).
     Untuk setiap DATA yang lolos checksum, simpan ke buffer sesuai
     seq_num-nya dan kirim balik ACK dengan seq_num yang sama.
     Kalau checksum gagal (payload korup), paket dibuang dan TIDAK di-ACK
     supaya sender melakukan retransmit setelah timeout.
  3. Setelah semua paket DATA terkumpul, terima FIN dan tulis file ke disk,
     lalu balas FIN_ACK.

Receiver ini disebut "selective repeat receiver" sederhana: menerima dan
mem-buffer paket di luar urutan, bukan cuma menolak apa pun yang tidak
sesuai next-expected-seq (itu ciri go-back-N).
"""

import argparse
import os
import socket
import sys

from protocol import (
    ChecksumError,
    MalformedPacketError,
    PacketType,
    decode_meta,
    decode_packet,
    encode_packet,
)

BUFFER_SIZE = 2048  # cukup untuk header 9B + MAX_PAYLOAD_SIZE 1024B + margin
DEFAULT_PORT = 12000
DEFAULT_OUTPUT_DIR = 'received_files'


def run_receiver(host: str, port: int, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((host, port))
    print(f'UDP reliable receiver siap, listening di {host}:{port}...')

    while True:
        try:
            _receive_one_file(server_socket, output_dir)
        except KeyboardInterrupt:
            print('\nReceiver dihentikan.')
            break


def _receive_one_file(server_socket: socket.socket, output_dir: str) -> None:
    """Terima satu sesi transfer file lengkap: META -> DATA* -> FIN."""
    filename = None
    filesize = None
    total_packets = None
    chunks: dict[int, bytes] = {}
    client_addr = None

    while True:
        try:
            raw, addr = server_socket.recvfrom(BUFFER_SIZE)
        except OSError as exc:
            print(f'Gagal recvfrom(): {exc}')
            continue

        try:
            pkt = decode_packet(raw)
        except ChecksumError:
            print(f'[{addr}] Paket korup (checksum gagal), dibuang, tidak di-ACK.')
            continue
        except MalformedPacketError as exc:
            print(f'[{addr}] Paket malformed, dibuang: {exc}')
            continue

        client_addr = addr

        if pkt.packet_type == PacketType.META:
            filename, filesize, total_packets = decode_meta(pkt.payload)
            chunks = {}
            print(f'[{addr}] META diterima: filename={filename!r}, '
                  f'filesize={filesize}, total_packets={total_packets}')
            ack = encode_packet(pkt.seq_num, PacketType.ACK)
            server_socket.sendto(ack, addr)

        elif pkt.packet_type == PacketType.DATA:
            if pkt.seq_num not in chunks:
                chunks[pkt.seq_num] = pkt.payload
                status = 'baru'
            else:
                status = 'duplikat (sudah ada)'
            print(f'[{addr}] DATA seq={pkt.seq_num} diterima ({len(pkt.payload)} byte), {status}')
            ack = encode_packet(pkt.seq_num, PacketType.ACK)
            server_socket.sendto(ack, addr)

        elif pkt.packet_type == PacketType.FIN:
            print(f'[{addr}] FIN diterima. Total chunk terkumpul: {len(chunks)}/{total_packets}')
            fin_ack = encode_packet(pkt.seq_num, PacketType.FIN_ACK)
            server_socket.sendto(fin_ack, addr)

            if (filename is not None and filesize is not None
                    and total_packets is not None and len(chunks) >= total_packets):
                _write_file(output_dir, filename, filesize, total_packets, chunks)
                return
            elif total_packets is None:
                print(f'[{addr}] FIN diterima tapi META belum pernah diterima, sesi diabaikan.')
                return
            else:
                missing = sorted(set(range(total_packets or 0)) - set(chunks.keys()))
                print(f'[{addr}] PERINGATAN: FIN diterima tapi chunk belum lengkap. '
                      f'Hilang: {missing}. Menunggu retransmit lanjutan dari sender...')
                # Tetap loop menunggu sender retransmit chunk yang hilang lalu
                # kirim FIN lagi; sesi belum ditutup.


def _write_file(output_dir: str, filename: str, filesize: int,
                 total_packets: int, chunks: dict[int, bytes]) -> None:
    safe_name = os.path.basename(filename)  # cegah path traversal dari nama file
    out_path = os.path.join(output_dir, safe_name)

    with open(out_path, 'wb') as f:
        for seq in range(total_packets):
            f.write(chunks[seq])

    written_size = os.path.getsize(out_path)
    status = 'OK' if written_size == filesize else 'UKURAN TIDAK COCOK'
    print(f'File selesai ditulis: {out_path} ({written_size} byte, '
          f'diharapkan {filesize} byte) [{status}]')


def main():
    parser = argparse.ArgumentParser(description='UDP reliable file transfer receiver')
    parser.add_argument('--host', default='', help='Bind address (default: semua interface)')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    run_receiver(args.host, args.port, args.output_dir)


if __name__ == '__main__':
    sys.exit(main())
