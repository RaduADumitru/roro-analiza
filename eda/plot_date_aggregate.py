"""EDA plotting script

Reads `SRC_01/date_agregate.csv` and creates three comparison plots
between the top-level folders `judete` and `raioane`:
- number of files
- total content chars
- total content words

Saves PNG files under `EDA/plots/`.
"""
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import sys


def load_data(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} does not exist")
    df = pd.read_csv(csv_path)
    return df


def get_top_level_values(df: pd.DataFrame, keys):
    """Return a dict key -> row (as Series) for rows where folder_path equals key.

    If a key is not present explicitly, attempt to aggregate rows that start with
    'key/' (i.e. the subfolders) as a fallback.
    """
    result = {}
    for k in keys:
        row = df.loc[df['folder_path'] == k]
        if not row.empty:
            result[k] = row.iloc[0]
            continue

        # fallback: sum over rows that start with 'k/'
        mask = df['folder_path'].str.startswith(f"{k}/")
        if mask.any():
            agg = df.loc[mask, ['files', 'total_content_chars', 'total_content_words']].sum()
            s = pd.Series({
                'folder_path': k,
                'files': int(agg['files']),
                'total_content_chars': int(agg['total_content_chars']),
                'total_content_words': int(agg['total_content_words'])
            })
            result[k] = s
        else:
            # last resort: use zeros
            result[k] = pd.Series({'folder_path': k, 'files': 0, 'total_content_chars': 0, 'total_content_words': 0})

    return result


def bar_plot(values: dict, field: str, ylabel: str, out_path: Path):
    labels = list(values.keys())
    data = [int(values[k].get(field, 0)) for k in labels]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, data, color=['#4c72b0', '#dd8452'])
    plt.title(f"{ylabel} — judete vs raioane")
    plt.ylabel(ylabel)
    for bar, val in zip(bars, data):
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, h, f"{val:,}", ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    repo_root = Path(__file__).resolve().parents[1]
    csv_path = repo_root / 'SRC_01' / 'date_agregate.csv'

    try:
        df = load_data(csv_path)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    keys = ['judete', 'raioane']
    vals = get_top_level_values(df, keys)

    out_dir = Path(__file__).resolve().parent / 'plots'

    bar_plot(vals, 'files', 'Number of files', out_dir / 'judete_vs_raioane_files.png')
    bar_plot(vals, 'total_content_chars', 'Total content chars', out_dir / 'judete_vs_raioane_chars.png')
    bar_plot(vals, 'total_content_words', 'Total content words', out_dir / 'judete_vs_raioane_words.png')

    print(f"Saved plots to {out_dir.resolve()}")


if __name__ == '__main__':
    main()
