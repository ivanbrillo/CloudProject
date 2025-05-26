import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

# === Constants ===
OUTPUT_DIR = './'
FILENAME = '../../invertedIndex/hadoop.csv'
SIZE_MAPPING = {
    '512KB': 512 * 1024,
    '1MB': 1 * 1024 * 1024,
    '512MB': 512 * 1024 * 1024,
    '1GB': 1 * 1024 * 1024 * 1024,
    '2GB': 2 * 1024 * 1024 * 1024,
}
BASE_COLORS = {1: 'tab:blue', 5: 'tab:orange', 15: 'tab:green'}
HATCH_MAP = {'10doc': '////', '20doc': None}
BYTES_TO_MB = lambda x: x / (1024 ** 2)
BYTES_TO_LABEL = lambda b: f"{int(b/1024)}KB" if b < 1024*1024 else f"{int(b/(1024*1024))}MB"


def load_and_preprocess_data(filename):
    df = pd.read_csv(filename)

    # Convert input-size e memory da byte a MB
    df['SIZE_BYTES'] = df['size'].map(SIZE_MAPPING)
    df['MEMORY_MB'] = df['memory'].apply(BYTES_TO_MB)

    # Aggrega usando MEMORY_MB al posto di memory
    agg = df.groupby(['doc', 'size', 'reducer'], as_index=False).agg({
        'SIZE_BYTES': 'first',
        'time': 'mean',
        'MEMORY_MB': 'mean'
    })
    agg.rename(columns={'MEMORY_MB': 'memory'}, inplace=True)

    agg.sort_values(['doc', 'SIZE_BYTES', 'reducer'], inplace=True)
    return agg


def plot_line_charts(agg, metric, ylabel, file_prefix):
    for dataset in agg['doc'].unique():
        plt.figure(figsize=(8, 6))
        subset = agg[agg['doc'] == dataset]
        for r in sorted(subset['reducer'].unique()):
            r_subset = subset[subset['reducer'] == r]
            plt.plot(
                r_subset['SIZE_BYTES'].apply(BYTES_TO_MB),
                r_subset[metric],
                marker='o' if metric == 'time' else 's',
                label=f"{r} Reducer{'s' if r > 1 else ''}"
            )

        title = f"HADOOP: {'Execution Time' if metric == 'time' else 'Memory Usage'} (with {dataset} as input)"
        plt.title(title)
        plt.xlabel('Input Size (MB)')
        plt.ylabel(ylabel)
        plt.xscale('log')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f'{file_prefix}_{dataset}.png'))
        plt.close()


def plot_comparison_bars(agg, metric, ylabel, title, fname):
    sizes = sorted(agg['SIZE_BYTES'].unique())
    labels = [BYTES_TO_LABEL(sz) for sz in sizes]
    reducers = sorted(agg['reducer'].unique())
    docs = ['10doc', '20doc']

    total_width = 0.75
    n_bars = len(reducers) * len(docs)
    bar_width = total_width / n_bars
    ind = np.arange(len(sizes))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()
    for i, sz in enumerate(sizes):
        for j, r in enumerate(reducers):
            for k, ds in enumerate(docs):
                subset = agg[(agg['SIZE_BYTES'] == sz) &
                             (agg['reducer'] == r) &
                             (agg['doc'] == ds)]
                value = subset[metric].values[0] if not subset.empty else 0
                group_gap = 0.2 * bar_width
                offset = (-total_width/2) + (j * len(docs) + k) * bar_width + j * group_gap + bar_width/2
                ax.bar(
                    ind[i] + offset,
                    value,
                    bar_width,
                    color=BASE_COLORS[r],
                    hatch=HATCH_MAP[ds],
                    edgecolor='black',
                    alpha=1.0 if ds == '20doc' else 0.6
                )

    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    ax.set_xlabel('Input Size')
    ax.set_ylabel(ylabel)
    ax.set_title(f'{title} 10doc vs 20doc')

    handles = []
    for r in reducers:
        handles.append(Patch(facecolor=BASE_COLORS[r], edgecolor=BASE_COLORS[r], label=f'{r} Reducers'))
    for ds in docs:
        hatch = HATCH_MAP[ds] or ''
        handles.append(Patch(facecolor='white', edgecolor='black', hatch=hatch, label=ds))

    ax.legend(handles=handles, loc='upper left')
    ax.grid(True, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, fname))
    plt.close()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    agg = load_and_preprocess_data(FILENAME)

    plot_line_charts(
        agg,
        metric='time',
        ylabel='Execution Time (ms)',
        file_prefix='execution_time'
    )
    plot_line_charts(
        agg,
        metric='memory',
        ylabel='Memory Usage (MB)',
        file_prefix='memory_usage'
    )

    plot_comparison_bars(
        agg,
        metric='time',
        ylabel='Mean Execution Time (ms)',
        title='HADOOP: Execution Time Comparison',
        fname='bar_execution_comparison.png'

    )
    plot_comparison_bars(
        agg,
        metric='memory',
        ylabel='Mean Memory Usage (MB)',
        title='HADOOP: Memory Usage Comparison',
        fname='bar_memory_comparison.png'
    )


if __name__ == '__main__':
    main()
