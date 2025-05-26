import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

# === Constants ===
OUTPUT_DIR = './'
FILENAME = '../../python/resultsPython.csv'
SIZE_MAPPING = {
    '512KB': 512 * 1024,
    '1MB': 1 * 1024 * 1024,
    '512MB': 512 * 1024 * 1024,
    '1GB': 1 * 1024 * 1024 * 1024,
    '2GB': 2 * 1024 * 1024 * 1024,
}
# Colori per i docs
DOC_COLORS = {'10doc': 'tab:blue', '20doc': 'tab:orange'}
HATCH_MAP = {'10doc': '////', '20doc': None}
BYTES_TO_LABEL = lambda b: f"{int(b/1024)}KB" if b < 1024*1024 else f"{int(b/(1024*1024))}MB"


def load_and_preprocess_data(filename):
    df = pd.read_csv(filename)

    # Convert input-size da label a byte count
    df['SIZE_BYTES'] = df['size'].map(SIZE_MAPPING)

    # Convert time da secondi a millisecondi
    df['TIME_MS'] = df['time']

    # Aggrega per doc e size
    agg = df.groupby(['doc', 'size'], as_index=False).agg({
        'SIZE_BYTES': 'first',
        'TIME_MS': 'mean'
    })

    # Rinomina colonne per chiarezza
    agg.sort_values(['doc', 'SIZE_BYTES'], inplace=True)
    return agg


def plot_comparison_bars(agg, metric, ylabel, fname):
    sizes = sorted(agg['SIZE_BYTES'].unique())
    labels = [BYTES_TO_LABEL(sz) for sz in sizes]
    docs = agg['doc'].unique()

    total_width = 0.75
    n_bars = len(docs)
    bar_width = total_width / n_bars
    ind = np.arange(len(sizes))

    plt.figure(figsize=(10, 7))
    ax = plt.gca()
    for i, sz in enumerate(sizes):
        for k, ds in enumerate(docs):
            subset = agg[(agg['SIZE_BYTES'] == sz) & (agg['doc'] == ds)]
            value = subset[metric].values[0] if not subset.empty else 0
            offset = (-total_width/2) + k * bar_width + bar_width/2
            ax.bar(
                ind[i] + offset,
                value,
                bar_width,
                color=DOC_COLORS.get(ds, 'gray'),
                hatch=HATCH_MAP[ds],
                edgecolor='black',
                alpha=1.0
            )

    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    ax.set_xlabel('Input Size')
    ax.set_ylabel(ylabel)
    ax.set_title(f'PYTHON: Execution Time Comparison 10doc vs 20doc')


    handles = []
    for ds in docs:
        handles.append(
            Patch(
                facecolor=DOC_COLORS.get(ds, 'gray'),
                edgecolor='black',
                hatch=HATCH_MAP[ds] or '',
                label=ds
            )
        )

    ax.legend(handles=handles, loc='upper left')
    ax.grid(True, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, fname))
    plt.close()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    agg = load_and_preprocess_data(FILENAME)

    # Plot Execution Time in ms
    plot_comparison_bars(
        agg,
        metric='TIME_MS',
        ylabel='Mean Execution Time (ms)',
        fname='bar_execution_comparison.png'
    )


if __name__ == '__main__':
    main()
