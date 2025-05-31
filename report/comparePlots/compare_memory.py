import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

# === Costanti ===
OUTPUT_DIR = './'
SIZE_MAPPING = {
    '512KB': 512 * 1024,
    '1MB': 1 * 1024 * 1024,
    '512MB': 512 * 1024 * 1024,
    '1GB': 1 * 1024 * 1024 * 1024,
    '2GB': 2 * 1024 * 1024 * 1024,
}

SYSTEM_COLORS = {
    'Hadoop':        'tab:blue',
    'Hadoop-InMapper':'tab:orange',
    'Spark':         'tab:green',
}

BYTES_TO_LABEL = lambda b: f"{int(b/1024)}KB" if b < 1024*1024 else f"{int(b/(1024*1024))}MB"


def load_filtered_csv(path, system_name, reducer=None, input_partitions=None):
    df = pd.read_csv(path)
    if reducer is not None and 'reducer' in df.columns:
        df = df[df['reducer'] == reducer]
    if input_partitions is not None and 'input_partitions' in df.columns:
        df = df[df['input_partitions'] == input_partitions]
    df['system'] = system_name
    return df


def preprocess_all_data():
    dfs = [
        load_filtered_csv('../../invertedIndex/hadoop2.csv', 
                          'Hadoop', reducer=5),
        load_filtered_csv('../../invertedIndex/hadoopInMapperCombiner2.csv', 
                          'Hadoop-InMapper', reducer=5),
        load_filtered_csv('../../spark/spark2.csv', 
                          'Spark', input_partitions=-1),

    ]

    df = pd.concat(dfs, ignore_index=True)
    df['SIZE_BYTES'] = df['size'].map(SIZE_MAPPING)
    df['TIME_MS'] = df['jobDurationMs']  # assuming già in ms

    # Media per system, doc e dimensione
    agg = df.groupby(['system', 'docSize', 'SIZE_BYTES'], as_index=False).agg({
        'aggregateResourceAllocation': 'mean',
        'TIME_MS': 'mean'
    })

    agg.sort_values(['docSize', 'SIZE_BYTES', 'system'], inplace=True)
    return agg


def plot_for_doc(agg, doc_label):
    subset = agg[agg['docSize'] == doc_label]
    sizes = sorted(subset['SIZE_BYTES'].unique())
    labels = [BYTES_TO_LABEL(sz) for sz in sizes]
    systems = ['Hadoop', 'Hadoop-InMapper', 'Spark']

    n_systems = len(systems)
    total_width = 0.8
    bar_width = total_width / n_systems
    x = np.arange(len(sizes))

    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    for i, system in enumerate(systems):
        times = []
        for sz in sizes:
            row = subset[(subset['system'] == system) & (subset['SIZE_BYTES'] == sz)]
            times.append(row['aggregateResourceAllocation'].values[0] if not row.empty else 0)
        ax.bar(
            x + (i - n_systems/2)*bar_width + bar_width/2,
            times,
            width=bar_width,
            label=system,
            color=SYSTEM_COLORS[system],
            edgecolor='black'
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel('Input Size')
    ax.set_ylabel('Aggregate Resource Allocation (MB*s)')
    ax.set_title(f'Aggregate Resource Allocation Comparison — with {doc_label} as input')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    plt.savefig(os.path.join(OUTPUT_DIR, f'comparison_mem_{doc_label}.png'))
    plt.close()


def main():
    agg = preprocess_all_data()
    # Genera due grafici: uno per 10doc e uno per 20doc
    for doc in ['10doc', '20doc']:
        plot_for_doc(agg, doc)


if __name__ == '__main__':
    main()
