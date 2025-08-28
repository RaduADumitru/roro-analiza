#!/usr/bin/env python3
"""

FISIER GENERAT DE ChatGPT

Cleanup script for Romanian news dataset.

Removes in-place:
  1. Files with empty "content".
  2. Files from Gazete with duplicate ratio > threshold.

Logs kept/removed files and per-gazeta stats.
"""

from pathlib import Path
from collections import defaultdict, Counter
import json, re, hashlib, csv

# --- helpers ---
word_re = re.compile(r"\w+", flags=re.UNICODE)

def words_count(s: str) -> int:
    if not isinstance(s, str):
        return 0
    return len(word_re.findall(s))

def content_hash(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return hashlib.sha1(s.strip().encode("utf-8")).hexdigest()

def gazeta_from_filename(name: str) -> str:
    return name.split("_", 1)[0] if "_" in name else name.rsplit(".", 1)[0]

def main():

    ROOT = Path('../data-cleaned')
    PATTERN = '*.json'
    THRESH = 0.5

    rows = []
    gazeta_hashes = defaultdict(list)
    gazeta_empty = Counter()

    processed = skipped = 0

    # --- scan all files ---
    for fp in ROOT.rglob(PATTERN):
        if not fp.is_file():
            continue
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[skip] {fp}: {e}")
            skipped += 1
            continue

        rel = fp.relative_to(ROOT).as_posix()
        fname = fp.name
        gz = gazeta_from_filename(fname)
        content = obj.get("content", "")
        wc = words_count(content)
        h  = content_hash(content)

        gazeta_hashes[gz].append(h)
        if wc == 0:
            gazeta_empty[gz] += 1

        rows.append({
            "relative_path": rel,
            "gazeta": gz,
            "filename": fname,
            "word_count": wc,
            "hash": h,
            "abs_path": str(fp),
        })
        processed += 1

    print(f"Scanned {processed} JSON files (skipped {skipped}).")

    # --- per-gazeta duplicate ratios ---
    stats = []
    for gz, hashes in gazeta_hashes.items():
        total = len(hashes)
        uniq  = len(set(hashes))
        dupes = total - uniq
        ratio = (dupes / total) if total else 0.0
        stats.append({
            "gazeta": gz,
            "total_files": total,
            "unique_contents": uniq,
            "duplicate_files": dupes,
            "dup_ratio": ratio,
            "empty_files": int(gazeta_empty[gz]),
        })
    stats.sort(key=lambda x: (-x["dup_ratio"], x["gazeta"]))

    banned_gazete = {s["gazeta"] for s in stats if s["dup_ratio"] > THRESH}
    print(f"Banned gazete (dup_ratio > {THRESH:g}): {sorted(banned_gazete)}")

    # --- decide removals ---
    kept, removed, errors = [], [], []
    for row in rows:
        reason = ""
        if row["word_count"] == 0:
            reason = "empty_content"
        elif row["gazeta"] in banned_gazete:
            reason = f"banned_gazeta_dup_ratio>{THRESH:g}"

        if reason:
            # remove file
            try:
                Path(row["abs_path"]).unlink()
                removed.append({**row, "reason": reason})
            except Exception as e:
                errors.append({"abs_path": row["abs_path"], "error": str(e)})
        else:
            kept.append(row)

    print(f"Removed {len(removed)} files, kept {len(kept)}. Errors: {len(errors)}")

    # --- save logs ---
    with open("removed_inplace.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["relative_path","gazeta","filename","word_count","reason"])
        w.writeheader()
        for r in removed:
            w.writerow({k:r.get(k,"") for k in w.fieldnames})

    with open("kept_inplace.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["relative_path","gazeta","filename","word_count"])
        w.writeheader()
        for r in kept:
            w.writerow({k:r.get(k,"") for k in w.fieldnames})

    with open("gazeta_stats_after_scan.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=stats[0].keys())
        w.writeheader()
        w.writerows(stats)

    if errors:
        with open("remove_errors.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["abs_path","error"])
            w.writeheader()
            w.writerows(errors)

if __name__ == "__main__":
    main()
