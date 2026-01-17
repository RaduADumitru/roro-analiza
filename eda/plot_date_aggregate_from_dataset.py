"""EDA plotting script

Calculates statistics from the dataset using RoRoParser and creates three
comparison plots between the top-level folders `judete` and `raioane`:
- number of files
- total content chars
- total content words

The plots include the name of the dataset used in the title.
Saves PNG files under `eda/plots/`.
"""
from pathlib import Path
import argparse
import sys
import matplotlib.pyplot as plt
from roro_module import RoRoParser


def flatten_entries(node):
    if node is None:
        return []
    if isinstance(node, dict):
        out = []
        for v in node.values():
            out.extend(flatten_entries(v))
        return out
    if isinstance(node, list):
        out = []
        for item in node:
            out.extend(flatten_entries(item))
        return out
    return [node]


def compute_statistics(entries):
    """Compute file count, total chars, and total words from entries.
    
    Returns:
        tuple: (file_count, total_chars, total_words)
    """
    file_count = len(entries)
    total_chars = 0
    total_words = 0
    
    for entry in entries:
        if not getattr(entry, 'text', None):
            continue
        text = entry.text
        total_chars += len(text)
        # Count words by splitting on whitespace
        total_words += len(text.split())
    
    return file_count, total_chars, total_words


def bar_plot(values: dict, field: str, ylabel: str, out_path: Path, dataset_name: str):
    """Create and save a bar plot comparing judete vs raioane.
    
    Parameters:
        values: dict with keys 'judete' and 'raioane', each containing stats
        field: the field name to plot ('files', 'total_chars', 'total_words')
        ylabel: label for the y-axis
        out_path: path to save the PNG file
        dataset_name: name of the dataset to include in the title
    """
    labels = list(values.keys())
    data = [int(values[k].get(field, 0)) for k in labels]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, data, color=['#4c72b0', '#dd8452'])
    plt.title(f"{ylabel} — judete vs raioane\n({dataset_name})")
    plt.ylabel(ylabel)
    for bar, val in zip(bars, data):
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, h, f"{val:,}", ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    parser_arg = argparse.ArgumentParser()
    parser_arg.add_argument('--data-path', default='data-cleaned', 
                            help='Path to dataset root (directory with JSON files)')
    args = parser_arg.parse_args()

    data_root = Path(args.data_path)
    
    # Make path absolute if relative
    if not data_root.is_absolute():
        repo_root = Path(__file__).resolve().parents[1]
        data_root = repo_root / data_root
    
    if not data_root.exists():
        print(f"Data path {data_root} does not exist. Please provide the path to the JSON dataset.")
        sys.exit(1)

    # Get dataset name from the path
    dataset_name = data_root.name

    # Configure parser
    parser_opts = {
        'path': str(data_root),
        'verbose': False,
        'use_spacy': False
    }

    parser = RoRoParser(parser_opts)
    try:
        parser.parse()
    except Exception as e:
        print('Parser failed:', e)
        sys.exit(1)

    keys = ['judete', 'raioane']
    values = {}

    for k in keys:
        node = parser.get(k)
        if node is None:
            print(f"Warning: {k} not found in dataset")
            values[k] = {'files': 0, 'total_chars': 0, 'total_words': 0}
            continue
        
        entries = flatten_entries(node)
        file_count, total_chars, total_words = compute_statistics(entries)
        values[k] = {
            'files': file_count,
            'total_chars': total_chars,
            'total_words': total_words,
            'n_entries': len(entries)
        }

    out_dir = Path(__file__).resolve().parent / 'plots'

    bar_plot(values, 'files', 'Number of files', 
             out_dir / f'judete_vs_raioane_files_{dataset_name}.png', dataset_name)
    bar_plot(values, 'total_chars', 'Total content chars', 
             out_dir / f'judete_vs_raioane_chars_{dataset_name}.png', dataset_name)
    bar_plot(values, 'total_words', 'Total content words', 
             out_dir / f'judete_vs_raioane_words_{dataset_name}.png', dataset_name)

    print(f"\nStatistics from {dataset_name}:")
    for k, v in values.items():
        print(f"- {k}: {v['files']:,} files, {v['total_chars']:,} chars, {v['total_words']:,} words")
    print(f"\nSaved plots to {out_dir.resolve()}")


if __name__ == '__main__':
    main()