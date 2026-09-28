"""
Benchmark perbandingan kinerja UDP reliable transfer (udp_sender/receiver)
vs TCP baseline (tcp_baseline_client/server), untuk beberapa ukuran file,
di localhost.

Cara pakai:
    1. Jalankan receiver UDP di terminal lain:
         python3 udp_receiver.py --port 12000
    2. Jalankan server TCP baseline di terminal lain:
         python3 tcp_baseline_server.py --port 12001
    3. Jalankan benchmark ini:
         python3 benchmark.py

Skrip ini HANYA menjalankan sisi sender/client (udp_sender.py dan
tcp_baseline_client.py) sebagai subprocess, karena receiver/server harus
sudah berjalan lebih dulu (levelnya sama seperti demo manual, supaya log
kedua sisi tetap konsisten dan bisa dicek manual kalau perlu).

Hasil disimpan ke results/benchmark_results.json dan
results/benchmark_chart.png.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

FILE_SIZES = {
    '100KB': 100 * 1024,
    '1MB': 1 * 1024 * 1024,
    '10MB': 10 * 1024 * 1024,
}

RESULTS_DIR = 'results'


def make_test_file(size_bytes: int, path: str) -> None:
    with open(path, 'wb') as f:
        f.write(os.urandom(size_bytes))


def run_udp_sender(filepath: str, host: str, port: int, loss_rate: float = 0.0) -> dict:
    cmd = [sys.executable, 'udp_sender.py', filepath,
           '--host', host, '--port', str(port), '--loss-rate', str(loss_rate)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return _parse_udp_output(result.stdout)


def run_tcp_client(filepath: str, host: str, port: int) -> dict:
    cmd = [sys.executable, 'tcp_baseline_client.py', filepath,
           '--host', host, '--port', str(port)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return _parse_tcp_output(result.stdout)


def _parse_udp_output(stdout: str) -> dict:
    def grab(pattern, cast=float):
        m = re.search(pattern, stdout)
        return cast(m.group(1)) if m else None

    return {
        'elapsed_seconds': grab(r'Waktu:\s*([\d.]+)\s*s'),
        'throughput_kbps': grab(r'Throughput:\s*([\d.]+)\s*KB/s'),
        'total_retransmits': grab(r'Total retransmit:\s*(\d+)', int),
        'simulated_drops': grab(r'Simulasi drop.*:\s*(\d+)', int),
    }


def _parse_tcp_output(stdout: str) -> dict:
    def grab(pattern, cast=float):
        m = re.search(pattern, stdout)
        return cast(m.group(1)) if m else None

    return {
        'elapsed_seconds': grab(r'Waktu:\s*([\d.]+)\s*s'),
        'throughput_kbps': grab(r'Throughput:\s*([\d.]+)\s*KB/s'),
    }


def main():
    parser = argparse.ArgumentParser(description='Benchmark UDP reliable vs TCP baseline')
    parser.add_argument('--udp-host', default='127.0.0.1')
    parser.add_argument('--udp-port', type=int, default=12000)
    parser.add_argument('--tcp-host', default='127.0.0.1')
    parser.add_argument('--tcp-port', type=int, default=12001)
    parser.add_argument('--loss-rate', type=float, default=0.0,
                         help='Simulasi packet loss untuk sisi UDP (0.0 = tanpa loss)')
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        for label, size_bytes in FILE_SIZES.items():
            filepath = os.path.join(tmpdir, f'testfile_{label}.bin')
            make_test_file(size_bytes, filepath)
            print(f'\n=== Ukuran file: {label} ({size_bytes} byte) ===')

            print('-> Menjalankan UDP reliable sender...')
            udp_stats = run_udp_sender(filepath, args.udp_host, args.udp_port, args.loss_rate)
            print(f'   {udp_stats}')

            print('-> Menjalankan TCP baseline client...')
            tcp_stats = run_tcp_client(filepath, args.tcp_host, args.tcp_port)
            print(f'   {tcp_stats}')

            results[label] = {'udp': udp_stats, 'tcp': tcp_stats, 'size_bytes': size_bytes}

    out_json = os.path.join(RESULTS_DIR, 'benchmark_results.json')
    with open(out_json, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\nHasil tersimpan: {out_json}')

    _plot_results(results)


def _plot_results(results: dict) -> None:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    labels = list(results.keys())
    udp_times = [results[l]['udp']['elapsed_seconds'] for l in labels]
    tcp_times = [results[l]['tcp']['elapsed_seconds'] for l in labels]
    udp_throughput = [results[l]['udp']['throughput_kbps'] for l in labels]
    tcp_throughput = [results[l]['tcp']['throughput_kbps'] for l in labels]

    x = range(len(labels))
    width = 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    # Skala log dipakai karena gap UDP reliable (overhead ACK per-paket di
    # userspace Python) vs TCP (reliability native di kernel) bisa 2 orde
    # magnitude di loopback -- skala linear membuat salah satu batang
    # nyaris tak terlihat.
    ax1.bar([i - width / 2 for i in x], udp_times, width, label='UDP reliable')
    ax1.bar([i + width / 2 for i in x], tcp_times, width, label='TCP')
    ax1.set_yscale('log')
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels)
    ax1.set_ylabel('Waktu transfer (s, skala log)')
    ax1.set_title('Waktu Transfer: UDP vs TCP')
    ax1.legend()

    ax2.bar([i - width / 2 for i in x], udp_throughput, width, label='UDP reliable')
    ax2.bar([i + width / 2 for i in x], tcp_throughput, width, label='TCP')
    ax2.set_yscale('log')
    ax2.set_xticks(list(x))
    ax2.set_xticklabels(labels)
    ax2.set_ylabel('Throughput (KB/s, skala log)')
    ax2.set_title('Throughput: UDP vs TCP')
    ax2.legend()

    fig.tight_layout()
    out_path = os.path.join(RESULTS_DIR, 'benchmark_chart.png')
    fig.savefig(out_path, dpi=150)
    print(f'Grafik tersimpan: {out_path}')


if __name__ == '__main__':
    sys.exit(main())
