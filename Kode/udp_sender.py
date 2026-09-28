"""
Sender (client) untuk reliable file transfer di atas UDP.

Mekanisme reliability + congestion control (selective repeat + AIMD sederhana):
  - File dipecah jadi paket DATA berukuran MAX_PAYLOAD_SIZE, seq_num 0..N-1.
  - Sender boleh punya sampai `cwnd` paket in-flight (belum di-ACK) sekaligus
    (sliding window). Selama masih ada slot window dan masih ada paket yang
    belum pernah dikirim, sender terus mengirim.
  - Setiap paket punya timer sendiri (per-packet timeout, bukan satu timer
    global) -> ciri selective repeat: kalau satu paket timeout, HANYA paket
    itu yang diretransmit, paket lain di window tetap dianggap valid.
  - Congestion window (cwnd) dimulai dari 1 dan naik +1 setiap ACK baru
    diterima (mirip TCP slow start, disederhanakan tanpa fase congestion
    avoidance penuh), dibatasi MAX_CWND. Setiap kali terjadi timeout,
    cwnd dipotong jadi cwnd // 2 (minimum 1) -- multiplicative decrease.
  - Setelah semua paket DATA ter-ACK, kirim FIN dan tunggu FIN_ACK
    (dengan retry) sebelum menutup sesi.

Opsi --loss-rate memungkinkan simulasi packet loss di SISI SENDER (paket
"dibuang" sebelum sungguh dikirim, secara acak) untuk mendemonstrasikan
mekanisme retransmit bekerja tanpa perlu tool jaringan eksternal.
"""

import argparse
import os
import random
import socket
import sys
import time

from protocol import (
    ChecksumError,
    MalformedPacketError,
    MAX_PAYLOAD_SIZE,
    PacketType,
    decode_packet,
    encode_meta,
    encode_packet,
)

DEFAULT_PORT = 12000
BUFFER_SIZE = 2048
INITIAL_CWND = 1
MAX_CWND = 32
TIMEOUT_SECONDS = 0.5
MAX_RETRIES_PER_PACKET = 10
FIN_MAX_RETRIES = 10


class ReliableUdpSender:
    def __init__(self, host: str, port: int, loss_rate: float = 0.0):
        self.host = host
        self.port = port
        self.loss_rate = loss_rate
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(TIMEOUT_SECONDS)
        self.cwnd = INITIAL_CWND
        self.cwnd_history: list[tuple[float, int]] = []  # (elapsed_s, cwnd) untuk laporan/grafik

    def send_file(self, filepath: str) -> dict:
        """Kirim satu file. Mengembalikan dict statistik untuk benchmark/laporan."""
        with open(filepath, 'rb') as f:
            data = f.read()

        chunks = [data[i:i + MAX_PAYLOAD_SIZE] for i in range(0, len(data), MAX_PAYLOAD_SIZE)]
        if not chunks:
            chunks = [b'']  # file kosong: tetap kirim 1 chunk kosong
        total_packets = len(chunks)
        filename = os.path.basename(filepath)
        filesize = len(data)

        start_time = time.monotonic()
        self.cwnd_history = [(0.0, self.cwnd)]

        self._send_meta(filename, filesize, total_packets)
        stats = self._send_all_chunks(chunks, start_time)
        self._send_fin()

        elapsed = time.monotonic() - start_time
        stats.update({
            'filename': filename,
            'filesize': filesize,
            'total_packets': total_packets,
            'elapsed_seconds': elapsed,
            'throughput_bytes_per_sec': filesize / elapsed if elapsed > 0 else float('inf'),
            'cwnd_history': self.cwnd_history,
        })
        return stats

    def _send_meta(self, filename: str, filesize: int, total_packets: int) -> None:
        payload = encode_meta(filename, filesize, total_packets)
        packet = encode_packet(seq_num=0, packet_type=PacketType.META, payload=payload)
        self._send_with_retry(packet, expect_seq=0, expect_type=PacketType.ACK,
                               label='META', max_retries=FIN_MAX_RETRIES)

    def _send_fin(self) -> None:
        packet = encode_packet(seq_num=0, packet_type=PacketType.FIN)
        self._send_with_retry(packet, expect_seq=0, expect_type=PacketType.FIN_ACK,
                               label='FIN', max_retries=FIN_MAX_RETRIES)

    def _send_with_retry(self, packet: bytes, expect_seq: int, expect_type: PacketType,
                          label: str, max_retries: int) -> None:
        for attempt in range(1, max_retries + 1):
            self.sock.sendto(packet, (self.host, self.port))
            try:
                raw, _ = self.sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                print(f'{label}: timeout menunggu balasan (attempt {attempt}/{max_retries}), retry...')
                continue
            try:
                reply = decode_packet(raw)
            except (ChecksumError, MalformedPacketError):
                continue
            if reply.packet_type == expect_type and reply.seq_num == expect_seq:
                print(f'{label}: balasan diterima, sesi lanjut.')
                return
        raise TimeoutError(f'{label}: tidak ada balasan valid setelah {max_retries} percobaan.')

    def _send_all_chunks(self, chunks: list[bytes], start_time: float) -> dict:
        """
        Selective-repeat sliding window: kirim paket sampai cwnd penuh,
        tunggu ACK, retransmit paket yang timeout, geser window saat paket
        base ter-ACK. Mengembalikan statistik jumlah retransmit & paket
        yang disimulasikan hilang.
        """
        total_packets = len(chunks)
        acked = [False] * total_packets
        send_time = {}       # seq -> waktu terakhir dikirim (untuk deteksi timeout)
        retry_count = {}     # seq -> berapa kali sudah dicoba
        simulated_drops = 0
        total_retransmits = 0

        next_to_send = 0
        base = 0  # paket paling kiri window yang belum ter-ACK

        while base < total_packets:
            # Kirim paket baru selama window masih punya slot.
            while next_to_send < total_packets and (next_to_send - base) < self.cwnd:
                self._transmit_chunk(chunks, next_to_send, send_time, retry_count)
                if random.random() < self.loss_rate:
                    simulated_drops += 1  # paket "hilang" (statistik saja, sudah terkirim di layer socket)
                next_to_send += 1

            try:
                raw, _ = self.sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                total_retransmits += self._handle_timeouts(chunks, acked, send_time, retry_count, base)
                continue

            try:
                reply = decode_packet(raw)
            except (ChecksumError, MalformedPacketError):
                continue

            if reply.packet_type != PacketType.ACK:
                continue

            seq = reply.seq_num
            if 0 <= seq < total_packets and not acked[seq]:
                acked[seq] = True
                self.cwnd = min(self.cwnd + 1, MAX_CWND)  # slow-start-like growth
                self.cwnd_history.append((time.monotonic() - start_time, self.cwnd))

            while base < total_packets and acked[base]:
                base += 1

        return {
            'total_retransmits': total_retransmits,
            'simulated_drops': simulated_drops,
        }

    def _transmit_chunk(self, chunks: list[bytes], seq: int,
                         send_time: dict, retry_count: dict) -> None:
        packet = encode_packet(seq_num=seq, packet_type=PacketType.DATA, payload=chunks[seq])
        if random.random() < self.loss_rate:
            # Simulasi loss: paket TIDAK benar-benar dikirim ke socket.
            send_time[seq] = time.monotonic()
            retry_count[seq] = retry_count.get(seq, 0)
            return
        self.sock.sendto(packet, (self.host, self.port))
        send_time[seq] = time.monotonic()
        retry_count[seq] = retry_count.get(seq, 0)

    def _handle_timeouts(self, chunks: list[bytes], acked: list[bool],
                          send_time: dict, retry_count: dict, base: int) -> int:
        """Retransmit semua paket in-flight yang sudah lewat TIMEOUT_SECONDS."""
        now = time.monotonic()
        retransmitted = 0
        for seq, t_sent in list(send_time.items()):
            if seq < base or acked[seq]:
                continue
            if now - t_sent < TIMEOUT_SECONDS:
                continue
            if retry_count.get(seq, 0) >= MAX_RETRIES_PER_PACKET:
                raise TimeoutError(f'Paket seq={seq} gagal dikirim setelah '
                                    f'{MAX_RETRIES_PER_PACKET} percobaan, transfer dibatalkan.')

            # Multiplicative decrease: kongesti terdeteksi lewat timeout.
            self.cwnd = max(1, self.cwnd // 2)

            packet = encode_packet(seq_num=seq, packet_type=PacketType.DATA, payload=chunks[seq])
            if random.random() >= self.loss_rate:
                self.sock.sendto(packet, (self.host, self.port))
            send_time[seq] = now
            retry_count[seq] = retry_count.get(seq, 0) + 1
            retransmitted += 1
            print(f'Timeout seq={seq}, retransmit (percobaan ke-{retry_count[seq]}), '
                  f'cwnd -> {self.cwnd}')
        return retransmitted


def main():
    parser = argparse.ArgumentParser(description='UDP reliable file transfer sender')
    parser.add_argument('filepath', help='Path file yang akan dikirim')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--loss-rate', type=float, default=0.0,
                         help='Probabilitas simulasi packet loss per paket, 0.0-1.0')
    args = parser.parse_args()

    sender = ReliableUdpSender(args.host, args.port, loss_rate=args.loss_rate)
    stats = sender.send_file(args.filepath)

    print('\n=== Statistik Transfer ===')
    print(f"File: {stats['filename']} ({stats['filesize']} byte, {stats['total_packets']} paket)")
    print(f"Waktu: {stats['elapsed_seconds']:.4f} s")
    print(f"Throughput: {stats['throughput_bytes_per_sec'] / 1024:.2f} KB/s")
    print(f"Total retransmit: {stats['total_retransmits']}")
    print(f"Simulasi drop (loss-rate): {stats['simulated_drops']}")


if __name__ == '__main__':
    sys.exit(main())
