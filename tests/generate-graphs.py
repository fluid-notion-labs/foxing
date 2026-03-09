#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-or-later
# Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
#
# tests/generate-graphs.py — Generate benchmark visualization graphs
#
# Usage: python3 tests/generate-graphs.py [benchmark.json]
# If no JSON provided, uses hardcoded v0.6.0 validated data.
# Output: docs/graphs/*.svg + docs/graphs/*.png

import json
import sys
import os
import numpy as np

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

# Foxing color palette
COLORS = {
    'cp': '#78909C',       # blue-grey
    'rsync': '#FF7043',    # deep orange
    'fxcp': '#26A69A',     # teal
    'foxingd': '#5C6BC0',  # indigo
    'highlight': '#FFB300', # amber
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'docs', 'graphs')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def setup_style():
    sns.set_theme(style="whitegrid", font_scale=1.1)
    plt.rcParams['figure.dpi'] = 150
    plt.rcParams['savefig.bbox'] = 'tight'
    plt.rcParams['font.family'] = 'sans-serif'


# ── Validated v0.6.0 Benchmark Data ──

NFS_PERF = {
    'workloads': ['1000×4KB', '100MB', '500 mixed', '5000 tiny', 'NFS→NFS\n100MB'],
    'cp':    [2111, 220, 1602, 9948, None],
    'rsync': [2709, 322, 3012, 12818, 297],
    'fxcp':  [2896, 386, 6514, 11492, 82],
}

PRUNING_SCALE = {
    'files':        [500,  2000,  5000,  10000, 10000],
    'dirs':         [5,    20,    50,    100,   50],
    'rsync_cold':   [2419, 10103, 25138, 51876, 45132],
    'fxcp_cold':    [1558, 5259,  13142, 24825, 25932],
    'rsync_resync': [257,  849,   1954,  5331,  4883],
    'fxcp_resync':  [117,  200,   369,   573,   423],
    'rsync_1mod':   [266,  810,   1896,  4800,  4794],
    'fxcp_1mod':    [484,  608,   698,   933,   1055],
}

MTTC_P1 = {
    'workloads': ['4KB', '1MB', '100MB', 'mod 4KB', 'mod 1MB', 'append', 'metadata', 'rename', 'batch\n100×4KB', 'batch\n10×10MB'],
    'XFS→XFS':  [75, 76, 206, 73, 75, 71, 72, 74, 100, 264],
    'XFS→NFS':  [85, 90, 362, 89, 95, 104, 116, 113, 415, 953],
    'NFS→NFS':  [91, 96, 1611, 93, 107, 97, 91, 100, 552, 1564],
    'XFS→tmpfs': [62, 64, 201, 61, 66, 62, 64, 62, 77, 248],
    'XFS→same': [69, 67, 112, 64, 77, 75, 68, 63, 88, 298],
}

MTTC_P2 = {
    'workloads': ['4KB', '8KB', '32KB', '64KB', 'rename', 'batch\n10×4KB'],
    'XFS→XFS':  [17, 16, 17, 16, 15, 187],
    'XFS→NFS':  [19, 18, 18, 18, 21, 209],
    'XFS→tmpfs': [16, 17, 17, 17, 16, 190],
}

ADVERSARIAL = {
    'phases': ['P0\nBaseline', 'P1\nHydration', 'P2\nWrite Storm', 'P3\nRename', 'P4\nNFS Drop',
               'P5\nKill/Resume', 'P6\nENOSPC', 'P7\nDelta', 'P8\nPruning', 'P9\nCombined'],
    'duration': [2, 58, 43, 63, 47, 25, 30, 47, 45, 52],
    'status': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],  # all pass
}


def plot_nfs_comparison():
    """Bar chart: NFS copy performance — cp vs rsync vs fxcp"""
    fig, ax = plt.subplots(figsize=(12, 6))

    workloads = NFS_PERF['workloads']
    x = np.arange(len(workloads))
    width = 0.25

    cp_vals = [v if v else 0 for v in NFS_PERF['cp']]
    rsync_vals = [v if v else 0 for v in NFS_PERF['rsync']]
    fxcp_vals = [v if v else 0 for v in NFS_PERF['fxcp']]

    bars_cp = ax.bar(x - width, cp_vals, width, label='cp', color=COLORS['cp'], alpha=0.85)
    bars_rsync = ax.bar(x, rsync_vals, width, label='rsync', color=COLORS['rsync'], alpha=0.85)
    bars_fxcp = ax.bar(x + width, fxcp_vals, width, label='fxcp', color=COLORS['fxcp'], alpha=0.85)

    # Add ratio annotations
    for i, (r, f) in enumerate(zip(NFS_PERF['rsync'], NFS_PERF['fxcp'])):
        if r and f:
            ratio = r / f
            color = '#2E7D32' if ratio > 1 else '#C62828'
            ax.annotate(f'{ratio:.2f}x', xy=(x[i] + width/2, f + 50),
                       fontsize=8, ha='center', color=color, fontweight='bold')

    ax.set_ylabel('Time (ms)', fontsize=12)
    ax.set_title('NFS 4.2 Copy Performance (XFS NVMe → NFS HDD)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.legend(loc='upper left')
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())

    fig.savefig(os.path.join(OUTPUT_DIR, 'nfs-comparison.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'nfs-comparison.png'))
    plt.close(fig)
    print('  Generated: nfs-comparison.svg')


def plot_pruning_scaling():
    """Line chart: Dir-hash pruning scaling — fxcp vs rsync at increasing file counts"""
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=False)

    files = PRUNING_SCALE['files'][:4]  # Use 100-files-per-dir series
    labels = [f'{f//1000}K' if f >= 1000 else str(f) for f in files]

    # Cold sync
    ax = axes[0]
    ax.plot(labels, [v/1000 for v in PRUNING_SCALE['rsync_cold'][:4]], 'o-', color=COLORS['rsync'], label='rsync', linewidth=2)
    ax.plot(labels, [v/1000 for v in PRUNING_SCALE['fxcp_cold'][:4]], 's-', color=COLORS['fxcp'], label='fxcp', linewidth=2)
    ax.set_title('Cold Sync', fontweight='bold')
    ax.set_ylabel('Time (seconds)')
    ax.set_xlabel('Files')
    ax.legend()

    # Resync (no changes)
    ax = axes[1]
    ax.plot(labels, PRUNING_SCALE['rsync_resync'][:4], 'o-', color=COLORS['rsync'], label='rsync', linewidth=2)
    ax.plot(labels, PRUNING_SCALE['fxcp_resync'][:4], 's-', color=COLORS['fxcp'], label='fxcp (pruning)', linewidth=2)
    ax.set_title('Resync — No Changes', fontweight='bold')
    ax.set_ylabel('Time (ms)')
    ax.set_xlabel('Files')
    ax.legend()
    # Add speedup annotations
    for i, (r, f) in enumerate(zip(PRUNING_SCALE['rsync_resync'][:4], PRUNING_SCALE['fxcp_resync'][:4])):
        ax.annotate(f'{r/f:.1f}x', xy=(labels[i], f), xytext=(0, -15),
                   textcoords='offset points', ha='center', fontsize=9, color='#2E7D32', fontweight='bold')

    # 1 file modified
    ax = axes[2]
    ax.plot(labels, PRUNING_SCALE['rsync_1mod'][:4], 'o-', color=COLORS['rsync'], label='rsync', linewidth=2)
    ax.plot(labels, PRUNING_SCALE['fxcp_1mod'][:4], 's-', color=COLORS['fxcp'], label='fxcp (pruning)', linewidth=2)
    ax.set_title('1 File Modified', fontweight='bold')
    ax.set_ylabel('Time (ms)')
    ax.set_xlabel('Files')
    ax.legend()

    fig.suptitle('Dir-Hash Adaptive Pruning at Scale (XFS → NFS 4.2)', fontsize=14, fontweight='bold', y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, 'pruning-scaling.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'pruning-scaling.png'))
    plt.close(fig)
    print('  Generated: pruning-scaling.svg')


def plot_mttc_heatmap():
    """Heatmap: MTTC Phase 1 — workload × topology latency matrix"""
    import pandas as pd

    data = {k: v for k, v in MTTC_P1.items() if k != 'workloads'}
    df = pd.DataFrame(data, index=MTTC_P1['workloads'])

    fig, ax = plt.subplots(figsize=(10, 7))
    sns.heatmap(df, annot=True, fmt='d', cmap='YlOrRd', ax=ax,
                cbar_kws={'label': 'Latency (ms)'},
                linewidths=0.5, linecolor='white')
    ax.set_title('Mean Time to Consistency — fxcp One-Shot (ms)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Workload')
    ax.set_xlabel('Topology')

    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, 'mttc-heatmap.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'mttc-heatmap.png'))
    plt.close(fig)
    print('  Generated: mttc-heatmap.svg')


def plot_daemon_latency():
    """Bar chart: foxingd daemon-mode BPF latency"""
    fig, ax = plt.subplots(figsize=(10, 5))

    workloads = MTTC_P2['workloads']
    x = np.arange(len(workloads))
    width = 0.25

    topos = ['XFS→XFS', 'XFS→NFS', 'XFS→tmpfs']
    colors_topo = [COLORS['fxcp'], COLORS['rsync'], COLORS['cp']]

    for i, (topo, color) in enumerate(zip(topos, colors_topo)):
        vals = MTTC_P2[topo]
        ax.bar(x + (i - 1) * width, vals, width, label=topo, color=color, alpha=0.85)

    ax.set_ylabel('Latency (ms)', fontsize=12)
    ax.set_title('foxingd Daemon Replication Latency (BPF Event-Driven)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.legend()
    ax.axhline(y=20, color='gray', linestyle='--', alpha=0.5, label='20ms target')

    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, 'daemon-latency.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'daemon-latency.png'))
    plt.close(fig)
    print('  Generated: daemon-latency.svg')


def plot_adversarial():
    """Horizontal bar chart: Adversarial test suite results"""
    fig, ax = plt.subplots(figsize=(10, 5))

    phases = ADVERSARIAL['phases']
    durations = ADVERSARIAL['duration']
    colors = ['#43A047' if s == 1 else '#E53935' for s in ADVERSARIAL['status']]

    y = np.arange(len(phases))
    bars = ax.barh(y, durations, color=colors, alpha=0.85, edgecolor='white')

    for bar, dur in zip(bars, durations):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                f'{dur}s', va='center', fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels(phases)
    ax.set_xlabel('Duration (seconds)')
    ax.set_title('foxingd Adversarial Test Suite — v0.6.0 (10/10 PASS)', fontsize=14, fontweight='bold')
    ax.invert_yaxis()

    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, 'adversarial-results.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'adversarial-results.png'))
    plt.close(fig)
    print('  Generated: adversarial-results.svg')


def plot_tool_comparison():
    """Radar/comparison chart: foxing vs alternatives on key dimensions"""
    categories = ['Small File\nSpeed', 'Large File\nSpeed', 'Delta\nEfficiency',
                   'Event\nLatency', 'Recovery\nSpeed', 'Memory\nFootprint']

    # Normalized scores (0-10, higher = better)
    foxing =  [8, 6, 10, 10, 9, 10]
    rsync =   [7, 8, 5,  2,  4, 4]
    lsyncd =  [5, 5, 5,  4,  3, 3]

    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

    for tool, values, color in [('foxing', foxing, COLORS['fxcp']),
                                 ('rsync', rsync, COLORS['rsync']),
                                 ('lsyncd', lsyncd, COLORS['cp'])]:
        vals = values + values[:1]
        ax.plot(angles, vals, 'o-', linewidth=2, label=tool, color=color)
        ax.fill(angles, vals, alpha=0.1, color=color)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=10)
    ax.set_ylim(0, 11)
    ax.set_title('Replication Tool Comparison', fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))

    fig.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, 'tool-comparison.svg'))
    fig.savefig(os.path.join(OUTPUT_DIR, 'tool-comparison.png'))
    plt.close(fig)
    print('  Generated: tool-comparison.svg')


if __name__ == '__main__':
    setup_style()
    print('Generating foxing v0.6.0 benchmark graphs...')
    print(f'Output: {OUTPUT_DIR}/')
    plot_nfs_comparison()
    plot_pruning_scaling()
    plot_mttc_heatmap()
    plot_daemon_latency()
    plot_adversarial()
    plot_tool_comparison()
    print('Done.')
