import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

# === Constants ===
OUTPUT_DIR = './'
FILENAME = '../../spark/spark.csv'  # punta al tuo file CSV
SIZE_MAPPING = {
    '512KB': 512 * 1024,
    '1MB': 1 * 1024 * 1024,
    '512MB': 512 * 1024 * 1024,
    '1GB': 1 * 1024 * 1024 * 1024,
    '2GB': 2 * 1024 * 1024 * 1024,
}
# Colori per ciascun numero di partition
PARTITION_COLORS = {
    -1: 'tab:blue',
     5: 'tab:orange',
    15: 'tab:green'
}
# Tratteggio per ciascun dataset size
HATCH_MAP = {
    '10doc': '////',
    '20doc': None
}
BYTES_TO_MB = lambda x: x / (1024 ** 2)
BYTES_TO_LABEL = lambda b: f"{int(b/1024)}KB" if b < 1024*1024 else f"{int(b/(1024*1024))}MB"

# === Utility Functions ===

def load_and_preprocess_data(filename):
    df = pd.read_csv(filename)
    df['SIZE_BYTES'] = df['size'].map(SIZE_MAPPING)
    
    # Assicurati di avere la colonna real_partition già nel CSV
    agg = df.groupby(['doc', 'size', 'input_partitions'], as_index=False).agg({
        'SIZE_BYTES': 'first',
        'time': 'mean',
        'real_partition': 'first'
    })
    agg.sort_values(['doc', 'SIZE_BYTES', 'input_partitions'], inplace=True)
    return agg


def plot_line_charts(agg, ylabel, file_prefix):
    for dataset in agg['doc'].unique():
        plt.figure(figsize=(8, 6))
        subset = agg[agg['doc'] == dataset]
        for p in sorted(subset['input_partitions'].unique()):
            p_sub = subset[subset['input_partitions'] == p]
            plt.plot(
                p_sub['SIZE_BYTES'].apply(BYTES_TO_MB),
                p_sub['time'],
                marker='o',
                color=PARTITION_COLORS.get(p, 'gray'),
                label="No Input Partition" if p == -1 else f"{p} Partitions"
            )
        plt.title(f'SPARK: Execution Time (with {dataset} as input)')
        plt.xlabel('Input Size (MB)')
        plt.ylabel(ylabel)
        plt.xscale('log')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f'{file_prefix}_{dataset}.png'))
        plt.close()

def plot_comparison_bars(agg, ylabel, fname):
    sizes      = sorted(agg['SIZE_BYTES'].unique())
    labels     = [BYTES_TO_LABEL(sz) for sz in sizes]
    partitions = sorted(agg['input_partitions'].unique())
    docs       = ['10doc', '20doc']

    total_width = 0.75
    n_bars      = len(partitions) * len(docs)
    bar_width   = total_width / n_bars
    ind         = np.arange(len(sizes))

    fig, ax = plt.subplots(figsize=(12, 7))

    for i, sz in enumerate(sizes):
        for j, p in enumerate(partitions):
            for k, ds in enumerate(docs):
                subset = agg[
                    (agg['SIZE_BYTES'] == sz) &
                    (agg['input_partitions'] == p) &
                    (agg['doc'] == ds)
                ]
                if subset.empty:
                    continue

                value   = subset['time'].iloc[0]
                real_p  = subset['real_partition'].iloc[0]

                # calcolo offset orizzontale
                group_gap = 0.2 * bar_width
                offset = (
                    - total_width/2 +
                    (j * len(docs) + k) * bar_width +
                    j * group_gap +
                    bar_width/2
                )
                x = ind[i] + offset

                # barra
                ax.bar(
                    x, value, bar_width,
                    color=PARTITION_COLORS.get(p, 'gray'),
                    hatch=HATCH_MAP.get(ds, ''),
                    edgecolor='black',
                    alpha=1.0 if ds == '20doc' else 0.6
                )
                
                ax.text(
                    x,
                    value * 1.02,
                    str(real_p),
                    ha='center',
                    va='bottom',
                    fontsize=9,
                    color='black'
                )

    # labeling
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    ax.set_xlabel('Input Size')
    ax.set_ylabel(ylabel)
    ax.set_title(f'SPARK: Execution Time Comparison 10doc vs 20doc')
    ax.grid(True, axis='y', alpha=0.5)

    # costruzione legend
    handles = []
    for p in partitions:
        lbl = "auto (-1)" if p == -1 else f"{p} partitions"
        handles.append(Patch(facecolor=PARTITION_COLORS[p],
                             edgecolor='black',
                             label=lbl))

    for ds in docs:
        handles.append(Patch(facecolor='white',
                             edgecolor='black',
                             hatch=HATCH_MAP[ds] or '',
                             label=ds))

    handles.append(Patch(facecolor='none',
                         edgecolor='none',
                         label='Numbers above bars: real partitions'))

    ax.legend(handles=handles,
              loc='upper left',
              frameon=True)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, fname))
    plt.close()

# === Main Execution ===
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    agg = load_and_preprocess_data(FILENAME)

    plot_line_charts(agg,
                     ylabel='Mean Execution Time (ms)',
                     file_prefix='exec_time')

    plot_comparison_bars(agg,
                         ylabel='Mean Execution Time (ms)',
                         fname='bar_exec_comparison.png')

if __name__ == '__main__':
    main()
