"""Plot average sentence length (words per sentence) for `judete` vs `raioane`.

This script reads the dataset using `RoRoParser` (text-only) and computes
average words-per-sentence by splitting text on sentence-ending punctuation
and counting word tokens (regex).

Outputs `eda/plots/avg_sentence_length_judete_vs_raioane.png`.
"""
from pathlib import Path
import argparse
import sys
import re
import matplotlib.pyplot as plt
from roro_module import RoRoParser


TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)
SENT_SPLIT_RE = re.compile(r"[\.\!\?]+")


def flatten_entries(node):
    if node is None:
        return []
    if isinstance(node, dict):
        out = []
        for v in node.values():
            out.extend(flatten_entries(v))
        return out
    return [node]


def compute_avg_words_per_sentence(entries):
    total_sentences = 0
    total_words = 0

    for e in entries:
        text = getattr(e, 'text', None)
        if not text:
            continue

        parts = SENT_SPLIT_RE.split(text)
        for sent in parts:
            words = TOKEN_RE.findall(sent.lower())
            if words:
                total_sentences += 1
                total_words += len(words)

    avg = (total_words / total_sentences) if total_sentences > 0 else 0.0
    return avg, total_sentences, total_words


def pick_values_from_text(parser, keys=('judete', 'raioane')):
    values = {}
    for k in keys:
        node = parser.get(k)
        entries = flatten_entries(node)
        avg, s_count, w_count = compute_avg_words_per_sentence(entries)
        values[k] = {'avg': avg, 'sentences': s_count, 'words': w_count, 'entries': len(entries)}
    return values


def plot_values(values: dict, out_path: Path):
    labels = list(values.keys())
    data = [values[k]['avg'] for k in labels]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, data, color=['#4c72b0', '#dd8452'])
    plt.title('Average words per sentence — judete vs raioane')
    plt.ylabel('Avg words per sentence')
    plt.ylim(0, 25)

    for bar, k in zip(bars, labels):
        val = values[k]['avg']
        plt.text(bar.get_x() + bar.get_width() / 2, val, f"{val:.2f}\n({values[k]['sentences']:,} s, {values[k]['words']:,} w)", ha='center', va='bottom')

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-path', default='ignore/data-work', help='Path to dataset root')
    args = p.parse_args()

    data_root = Path(args.data_path)
    if not data_root.exists():
        print(f"Data path {data_root} does not exist. Provide the dataset root path.")
        sys.exit(1)

    parser_opts = {'path': str(data_root), 'verbose': False, 'use_spacy': False}

    parser = RoRoParser(parser_opts)
    try:
        parser.parse()
    except Exception as e:
        print('Parser failed:', e)
        sys.exit(1)

    values = pick_values_from_text(parser, keys=('judete', 'raioane'))

    out_path = Path(__file__).resolve().parent / 'plots' / 'avg_sentence_length_judete_vs_raioane.png'
    plot_values(values, out_path)

    print('Average words-per-sentence:')
    for k, v in values.items():
        print(f"- {k}: {v['avg']:.3f} ({v['sentences']:,} sentences, {v['words']:,} words, {v['entries']} entries)")
    print(f"Saved plot to {out_path.resolve()}")


if __name__ == '__main__':
    main()
