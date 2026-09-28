"""
Protokol aplikasi untuk reliable file transfer di atas UDP.

Setiap paket UDP dibungkus dengan header biner berukuran tetap (fixed-size),
diikuti payload (boleh kosong untuk ACK/FIN). Header memisahkan informasi
kendali (sequence number, tipe paket, checksum) dari data aplikasi (payload),
sesuai requirement "pemisahan data & informasi kendali" pada brief tugas.

Format header (big-endian, total 9 byte):
    seq_num     : 4 byte unsigned int   -> nomor urut paket
    packet_type : 1 byte unsigned int   -> lihat PacketType di bawah
    checksum    : 4 byte unsigned int   -> CRC32 dari payload saja

Payload mengikuti header persis sepanjang sisa datagram (tidak perlu length
prefix terpisah karena UDP recvfrom() sudah mengembalikan tepat satu
datagram/message boundary).
"""

import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum


HEADER_FORMAT = '!IBI'  # seq_num (4B), packet_type (1B), checksum (4B)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Ukuran payload maksimum per paket DATA. Dipilih di bawah MTU umum (1500 byte)
# dikurangi header IP/UDP/aplikasi, supaya aman dari fragmentasi IP di jaringan
# lokal maupun internet biasa.
MAX_PAYLOAD_SIZE = 1024


class PacketType(IntEnum):
    """Tipe paket. META/ACK/FIN adalah paket kendali; DATA adalah paket data."""
    META = 0      # sender -> receiver: metadata file (nama, ukuran, total paket)
    DATA = 1      # sender -> receiver: satu chunk isi file
    ACK = 2       # receiver -> sender: konfirmasi seq_num tertentu diterima OK
    FIN = 3       # sender -> receiver: semua data sudah dikirim, minta tutup sesi
    FIN_ACK = 4   # receiver -> sender: konfirmasi FIN diterima, sesi boleh ditutup


class ChecksumError(ValueError):
    """Payload paket korup: checksum yang dihitung ulang tidak cocok dengan header."""


class MalformedPacketError(ValueError):
    """Paket lebih pendek dari HEADER_SIZE, tidak bisa di-decode sebagai header."""


@dataclass
class Packet:
    seq_num: int
    packet_type: PacketType
    payload: bytes

    def encode(self) -> bytes:
        checksum = zlib.crc32(self.payload) & 0xFFFFFFFF
        header = struct.pack(HEADER_FORMAT, self.seq_num, int(self.packet_type), checksum)
        return header + self.payload


def encode_packet(seq_num: int, packet_type: PacketType, payload: bytes = b'') -> bytes:
    """Bungkus (seq_num, packet_type, payload) jadi bytes siap kirim lewat UDP."""
    return Packet(seq_num, packet_type, payload).encode()


def decode_packet(data: bytes) -> Packet:
    """
    Bongkar bytes hasil recvfrom() jadi Packet. Memvalidasi checksum payload.

    Raises:
        MalformedPacketError: data lebih pendek dari header, kemungkinan
            paket rusak/terpotong di jaringan.
        ChecksumError: payload tidak cocok dengan checksum di header,
            kemungkinan bit-flip/korupsi di jaringan.
    """
    if len(data) < HEADER_SIZE:
        raise MalformedPacketError(
            f'Paket terlalu pendek: {len(data)} byte, minimal {HEADER_SIZE} byte'
        )

    seq_num, raw_type, checksum = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    payload = data[HEADER_SIZE:]

    computed = zlib.crc32(payload) & 0xFFFFFFFF
    if computed != checksum:
        raise ChecksumError(
            f'Checksum tidak cocok untuk seq={seq_num}: '
            f'header={checksum:#010x}, computed={computed:#010x}'
        )

    try:
        packet_type = PacketType(raw_type)
    except ValueError as exc:
        raise MalformedPacketError(f'packet_type tidak dikenal: {raw_type}') from exc

    return Packet(seq_num, packet_type, payload)


def encode_meta(filename: str, filesize: int, total_packets: int) -> bytes:
    """Payload META: '<total_packets>|<filesize>|<filename>' sebagai UTF-8."""
    name_bytes = filename.encode('utf-8')
    return f'{total_packets}|{filesize}|'.encode('ascii') + name_bytes


def decode_meta(payload: bytes) -> tuple[str, int, int]:
    """Kebalikan encode_meta -> (filename, filesize, total_packets)."""
    # Split hanya 2 kali karena filename sendiri boleh mengandung karakter '|'.
    header_bytes, _, remainder = payload.partition(b'|')
    total_packets_str = header_bytes.decode('ascii')
    filesize_str, _, filename_bytes = remainder.partition(b'|')
    filename = filename_bytes.decode('utf-8')
    return filename, int(filesize_str), int(total_packets_str)
