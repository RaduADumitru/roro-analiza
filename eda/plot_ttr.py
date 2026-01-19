"""Plot Type-Token Ratio (TTR) for `judete` vs `raioane`.

This script supports two tokenization modes controlled by the `--use-spacy` flag:
- when not set: tokenization by regex words (`\w+`) — output file
    `ttr_words_judete_vs_raioane_<dataset>.png`
- when set: tokenization using spaCy tokens (`token.is_alpha`) — output file
    `ttr_spacy_tokens_judete_vs_raioane_<dataset>.png`
Each plot title also includes the dataset name.
"""
from pathlib import Path
import re
import argparse
import sys
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


def compute_ttr_words(entries):
    total_tokens = 0
    types = set()
    for e in entries:
        if not getattr(e, 'text', None):
            continue
        words = TOKEN_RE.findall(e.text.lower())
        total_tokens += len(words)
        types.update(words)

    ttr = (len(types) / total_tokens) if total_tokens > 0 else 0.0
    return ttr, len(types), total_tokens


def compute_ttr_spacy(entries):
    total_tokens = 0
    types = set()
    for e in entries:
        doc = getattr(e, 'doc', None)
        if doc is None:
            # skip entries without spaCy doc
            continue
        tokens = [t.text.lower() for t in doc if t.is_alpha]
        total_tokens += len(tokens)
        types.update(tokens)

    ttr = (len(types) / total_tokens) if total_tokens > 0 else 0.0
    return ttr, len(types), total_tokens


def plot_ttr(results: dict, out_path: Path, title: str):
    labels = list(results.keys())
    ttrs = [results[k]['ttr'] * 100.0 for k in labels]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, ttrs, color=['#4c72b0', '#dd8452'])
    plt.ylim(0, max(ttrs) * 1.2 if ttrs else 1)
    plt.ylabel('Type-Token Ratio (%)')
    plt.title(title)

    for bar, k in zip(bars, labels):
        h = bar.get_height()
        types = results[k]['types']
        tokens = results[k]['tokens']
        plt.text(bar.get_x() + bar.get_width() / 2, h, f"{h:.3f}%\n({types:,}/{tokens:,})", ha='center', va='bottom', fontsize=9)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-path', default='data-cleaned', help='Path to dataset root (directory with JSON files)')
    p.add_argument('--use-spacy', action='store_true', help='Use spaCy tokenization (requires model installed)')
    p.add_argument('--spacy-model', default='ro_core_news_sm', help='spaCy model to load when --use-spacy is set')
    args = p.parse_args()

    data_root = Path(args.data_path)

    # Resolve relative paths against repo root for convenience
    if not data_root.is_absolute():
        repo_root = Path(__file__).resolve().parents[1]
        data_root = repo_root / data_root

    if not data_root.exists():
        print(f"Data path {data_root} does not exist. Please provide the path to the JSON dataset.")
        sys.exit(1)

    dataset_name = data_root.name

    # Configure parser
    parser_opts = {'path': str(data_root), 'verbose': False, 'use_spacy': bool(args.use_spacy)}
    if args.use_spacy:
        parser_opts['spacy_model_name'] = args.spacy_model

    parser = RoRoParser(parser_opts)
    try:
        parser.parse()
    except Exception as e:
        print('Parser failed:', e)
        sys.exit(1)

    keys = ['judete', 'raioane']
    results = {}

    if args.use_spacy:
        for k in keys:
            node = parser.get(k)
            entries = flatten_entries(node)
            ttr, types_count, tokens_count = compute_ttr_spacy(entries)
            results[k] = {'ttr': ttr, 'types': types_count, 'tokens': tokens_count, 'n_entries': len(entries)}

        out_path = Path(__file__).resolve().parent / 'plots' / f'ttr_spacy_tokens_judete_vs_raioane_{dataset_name}.png'
        plot_ttr(results, out_path, f'TTR (spaCy tokens): judete vs raioane\n({dataset_name})')
    else:
        for k in keys:
            node = parser.get(k)
            entries = flatten_entries(node)
            ttr, types_count, tokens_count = compute_ttr_words(entries)
            results[k] = {'ttr': ttr, 'types': types_count, 'tokens': tokens_count, 'n_entries': len(entries)}

        out_path = Path(__file__).resolve().parent / 'plots' / f'ttr_words_judete_vs_raioane_{dataset_name}.png'
        plot_ttr(results, out_path, f'TTR (words): judete vs raioane\n({dataset_name})')

    print('TTR results:')
    for k, v in results.items():
        print(f"- {k}: TTR={v['ttr']:.6f} ({v['types']:,}/{v['tokens']:,} tokens), entries={v['n_entries']}")
    print(f"Saved plot to {out_path.resolve()} ({dataset_name})")


if __name__ == '__main__':
    main()
