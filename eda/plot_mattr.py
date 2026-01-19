"""Plot Moving-Average Type-Token Ratio (MATTR) for `judete` vs `raioane`.

MATTR computes TTR over a sliding window of N tokens and averages across all windows.
This gives a more stable measure than raw TTR, especially for texts of varying length.

Outputs `eda/plots/mattr_window_{window_size}_judete_vs_raioane_<dataset>.png` with the dataset name in the title.
"""
from pathlib import Path
import argparse
import sys
import re
import matplotlib.pyplot as plt
from roro_module import RoRoParser


TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)


def flatten_entries(node):
    if node is None:
        return []
    if isinstance(node, dict):
        out = []
        for v in node.values():
            out.extend(flatten_entries(v))
        return out
    return [node]


def compute_mattr(entries, window_size=100):
    """Compute Moving-Average Type-Token Ratio.
    
    Concatenates all entries into a single token list, then slides a window of `window_size` tokens
    and computes TTR for each window. Returns the average of all window TTRs.
    
    Returns: MATTR (float), total_windows (int), total_tokens (int)
    """
    # Concatenate all tokens from all entries
    all_tokens = []
    for e in entries:
        text = getattr(e, 'text', None)
        if not text:
            continue
        tokens = [w.lower() for w in TOKEN_RE.findall(text)]
        all_tokens.extend(tokens)
    
    total_tokens = len(all_tokens)
    
    if total_tokens < window_size:
        # If entire corpus is shorter than window, compute single TTR
        if all_tokens:
            ttr = len(set(all_tokens)) / total_tokens
            return ttr, 1, total_tokens
        return 0.0, 0, 0
    
    # Slide window over the entire concatenated token list
    all_window_ttrs = []
    for i in range(total_tokens - window_size + 1):
        window = all_tokens[i:i + window_size]
        ttr = len(set(window)) / window_size
        all_window_ttrs.append(ttr)
    
    mattr = sum(all_window_ttrs) / len(all_window_ttrs) if all_window_ttrs else 0.0
    return mattr, len(all_window_ttrs), total_tokens


def compute_mattr_for_keys(parser, keys, window_size):
    """Compute MATTR for each key with the given window size.
    
    Returns: dict[key] = {'mattr': float, 'windows': int, 'tokens': int, 'entries': int}
    """
    results = {}
    for k in keys:
        node = parser.get(k)
        entries = flatten_entries(node)
        mattr, windows, tokens = compute_mattr(entries, window_size=window_size)
        results[k] = {'mattr': mattr, 'windows': windows, 'tokens': tokens, 'entries': len(entries)}
    return results


def plot_mattr_bar(results, window_size, out_path: Path, dataset_name: str):
    """Plot MATTR as a bar chart comparing judete vs raioane."""
    keys = list(results.keys())
    mattr_values = [results[k]['mattr'] * 100 for k in keys]
    
    plt.figure(figsize=(6, 4))
    bars = plt.bar(keys, mattr_values, color=['#4c72b0', '#dd8452'])
    plt.ylim(0, max(mattr_values) * 1.2 if mattr_values else 1)
    plt.ylabel('MATTR (%)')
    plt.title(f'MATTR (window={window_size}) — judete vs raioane\n({dataset_name})')
    
    for bar, k in zip(bars, keys):
        h = bar.get_height()
        windows = results[k]['windows']
        tokens = results[k]['tokens']
        plt.text(bar.get_x() + bar.get_width() / 2, h, 
                f"{h:.3f}%\n({windows:,} windows\n{tokens:,} tokens)", 
                ha='center', va='bottom', fontsize=9)
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-path', default='data-cleaned', help='Path to dataset root')
    p.add_argument('--window-size', type=int, default=100, help='Window size for MATTR computation (default: 100)')
    args = p.parse_args()

    data_root = Path(args.data_path)

    # Make path absolute if provided relative to repo root
    if not data_root.is_absolute():
        repo_root = Path(__file__).resolve().parents[1]
        data_root = repo_root / data_root

    if not data_root.exists():
        print(f"Data path {data_root} does not exist. Provide the dataset root path.")
        sys.exit(1)

    dataset_name = data_root.name
    
    parser_opts = {'path': str(data_root), 'verbose': False, 'use_spacy': False}
    parser = RoRoParser(parser_opts)
    try:
        parser.parse()
    except Exception as e:
        print('Parser failed:', e)
        sys.exit(1)

    keys = ['judete', 'raioane']
    results = compute_mattr_for_keys(parser, keys, args.window_size)

    out_path = Path(__file__).resolve().parent / 'plots' / f'mattr_window_{args.window_size}_judete_vs_raioane_{dataset_name}.png'
    plot_mattr_bar(results, args.window_size, out_path, dataset_name)

    print('MATTR results:')
    for k in keys:
        v = results[k]
        print(f"- {k}: MATTR={v['mattr']:.6f} ({v['windows']:,} windows, {v['tokens']:,} tokens, {v['entries']:,} entries)")
    print(f"\nSaved plot to {out_path.resolve()} ({dataset_name})")


if __name__ == '__main__':
    main()
