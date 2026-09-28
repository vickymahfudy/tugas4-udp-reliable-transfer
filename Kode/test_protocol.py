"""Unit test untuk protocol.py (encode/decode paket + meta, deteksi korupsi)."""

import unittest

from protocol import (
    ChecksumError,
    MalformedPacketError,
    PacketType,
    decode_meta,
    decode_packet,
    encode_meta,
    encode_packet,
)


class TestPacketRoundTrip(unittest.TestCase):
    def test_data_packet_round_trip(self):
        raw = encode_packet(seq_num=7, packet_type=PacketType.DATA, payload=b'halo dunia')
        pkt = decode_packet(raw)
        self.assertEqual(pkt.seq_num, 7)
        self.assertEqual(pkt.packet_type, PacketType.DATA)
        self.assertEqual(pkt.payload, b'halo dunia')

    def test_ack_packet_has_empty_payload(self):
        raw = encode_packet(seq_num=3, packet_type=PacketType.ACK)
        pkt = decode_packet(raw)
        self.assertEqual(pkt.seq_num, 3)
        self.assertEqual(pkt.packet_type, PacketType.ACK)
        self.assertEqual(pkt.payload, b'')

    def test_fin_and_fin_ack_round_trip(self):
        for ptype in (PacketType.FIN, PacketType.FIN_ACK):
            raw = encode_packet(seq_num=99, packet_type=ptype)
            pkt = decode_packet(raw)
            self.assertEqual(pkt.packet_type, ptype)

    def test_large_seq_num(self):
        # Pastikan seq_num 32-bit besar tidak overflow/salah pack.
        raw = encode_packet(seq_num=4_000_000_000, packet_type=PacketType.DATA, payload=b'x')
        pkt = decode_packet(raw)
        self.assertEqual(pkt.seq_num, 4_000_000_000)


class TestChecksumDetection(unittest.TestCase):
    def test_corrupted_payload_raises_checksum_error(self):
        raw = encode_packet(seq_num=1, packet_type=PacketType.DATA, payload=b'data asli')
        # Simulasikan bit-flip di jaringan: ubah satu byte payload setelah header.
        corrupted = bytearray(raw)
        corrupted[-1] ^= 0xFF
        with self.assertRaises(ChecksumError):
            decode_packet(bytes(corrupted))

    def test_truncated_packet_raises_malformed_error(self):
        with self.assertRaises(MalformedPacketError):
            decode_packet(b'\x00\x01')  # jauh lebih pendek dari HEADER_SIZE

    def test_unknown_packet_type_raises_malformed_error(self):
        raw = encode_packet(seq_num=1, packet_type=PacketType.DATA, payload=b'x')
        corrupted = bytearray(raw)
        corrupted[4] = 255  # byte packet_type diganti nilai tidak dikenal
        with self.assertRaises(MalformedPacketError):
            decode_packet(bytes(corrupted))


class TestMetaEncoding(unittest.TestCase):
    def test_meta_round_trip_simple_name(self):
        raw = encode_meta(filename='laporan.pdf', filesize=123456, total_packets=42)
        filename, filesize, total_packets = decode_meta(raw)
        self.assertEqual(filename, 'laporan.pdf')
        self.assertEqual(filesize, 123456)
        self.assertEqual(total_packets, 42)

    def test_meta_round_trip_filename_with_pipe_char(self):
        # Filename yang (secara tidak umum) mengandung '|' harus tetap utuh
        # karena partition() dibatasi hanya split 2 field pertama.
        raw = encode_meta(filename='a|b|c.txt', filesize=10, total_packets=1)
        filename, filesize, total_packets = decode_meta(raw)
        self.assertEqual(filename, 'a|b|c.txt')
        self.assertEqual(filesize, 10)
        self.assertEqual(total_packets, 1)

    def test_meta_packet_end_to_end(self):
        meta_payload = encode_meta('data.bin', 999, 5)
        raw = encode_packet(seq_num=0, packet_type=PacketType.META, payload=meta_payload)
        pkt = decode_packet(raw)
        self.assertEqual(pkt.packet_type, PacketType.META)
        filename, filesize, total_packets = decode_meta(pkt.payload)
        self.assertEqual((filename, filesize, total_packets), ('data.bin', 999, 5))


if __name__ == '__main__':
    unittest.main()
