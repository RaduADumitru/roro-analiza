
from pathlib import Path
import random
from collections import defaultdict
import spacy
import re 
import json
import sqlite3
import hashlib
import csv



""" 
Class RoRoShuffler
 - Loads data from the Parser class
 - For each requested level,
  -- splits text into sentences, 
  -- removes duplicates,
  -- shuffles them
  -- recombines them into texts with target number of words.
"""

class RoRoShufflerDatabase:
    def __init__(self, parser, **kwargs):
        self.parser = parser

        self.level = kwargs.get("level", -1)
        self.text_target_word_count = kwargs.get("target_word_count", 2000)
        self.output_path = kwargs.get("output_path", "shuffler_output") 

        self.spacy_model_name = kwargs.get("spacy_model", "ro_core_news_sm")
        self.batch_size = kwargs.get("batch_size", 512)
        self.seed = kwargs.get("seed", 42)

        self.commit_every = kwargs.get("commit_every", 512) # sentences between database commits

        self._spacy_model = None

    def run(self):

        entries = self.parser.get_flat()
        out_root = Path(self.output_path)
        out_root.mkdir(parents=True, exist_ok=True)

        has_spacy = bool(entries) and getattr(entries[0], "doc", None) is not None

        if not has_spacy:
            self._spacy_model = spacy.load(self.spacy_model_name, disable=["ner", "lemmatizer", "textcat"])
            if ("parser" not in self._spacy_model.pipe_names and
                "senter" not in self._spacy_model.pipe_names and
                "sentencizer" not in self._spacy_model.pipe_names):
                self._spacy_model.add_pipe("sentencizer")


        folder_to_idxs = defaultdict(list)
        for i, e in enumerate(entries):
            rel_path = e.meta.get("rel_path", "")
            subpath = self._subpath_from_rel_path(rel_path, self.level)
            folder_to_idxs[subpath].append(i)

        # sqlite
        db_path = out_root / "_sents.sqlite"
        conn = self._open_db(db_path)

        folder_stats = {}
        all_folders = sorted(folder_to_idxs.keys())

        for subpath in all_folders:

            print (f"Indexing {subpath}")

            # Cleanup for this folder
            conn.execute("DELETE FROM sents WHERE subpath = ?", (str(subpath),))
            conn.commit()


            total_sents = 0
            unique_sents = 0

            conn.execute("BEGIN")
            idxs = folder_to_idxs[subpath]

            total_entries = len(idxs)
            processed_entries = 0

            rejected = [] 
            for start in range(0, len(idxs), self.batch_size):
                batch_entries = [entries[j] for j in idxs[start:start + self.batch_size]]
                

                pct = processed_entries / total_entries * 100 if total_entries else 100
                print(f"---Indexing progress: {processed_entries}/{total_entries} entries ({pct:.2f}%) | total_sents={total_sents} unique={unique_sents}")
                processed_entries += len(batch_entries)

                for raw in self._iter_sent_texts_batch(batch_entries, has_spacy):

                    s = raw.strip()
                    if not s:
                        continue

                    ok, reason = self._is_good_sentence(s)
                    if not ok:
                        rejected.append((reason, s))
                        continue


                    total_sents += 1

                    key = self._norm_sent(s)
                    if not key: 
                        continue

                    h = self._sent_hash(key)
                    order = self._sent_randkey(h)

                    conn.execute( """
                    INSERT INTO sents(subpath, h, r, text, cnt)
                    VALUES(?, ?, ?, ?, 1)
                    ON CONFLICT(subpath, h) DO UPDATE SET
                        cnt = cnt + 1
                    """, (str(subpath), h, order, s))

                    if total_sents % self.commit_every == 0:
                        conn.execute("COMMIT")

                        (unique_sents,) = conn.execute(
                            "SELECT COUNT(*) FROM sents WHERE subpath = ?",
                            (str(subpath),)
                        ).fetchone()
                        
                        conn.execute("BEGIN")

                    

            conn.execute("COMMIT")

            (unique_sents,) = conn.execute(
                "SELECT COUNT(*) FROM sents WHERE subpath = ?",
                (str(subpath),)
            ).fetchone()

            print(f"---Indexing progress: {processed_entries}/{total_entries} entries (100%) | total_sents={total_sents} unique={unique_sents}")
                
            


            out_dir = out_root / subpath
            out_dir.mkdir(parents=True, exist_ok=True)


            top100 = conn.execute(
                """
                SELECT cnt, text
                FROM sents
                WHERE subpath = ?
                ORDER BY cnt DESC
                LIMIT 100
                """,
                (str(subpath),)
            ).fetchall()

            csv_path = out_dir / "_top100_duplicates.csv"

            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["rank", "count", "sentence"])

                for i, (cnt, text) in enumerate(top100, start=1):
                    writer.writerow([i, cnt, text])

            print(f"Most common sentence (appeared {top100[0][0]} times): \n{top100[0][1]}")


            rej_csv = out_dir / "_rejected_sentences.csv"

            with rej_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["reason", "sentence"])
                for reason, sent in rejected:
                    writer.writerow([reason, sent])

            print(f"Rejected {len(rejected)} sentences")

             # write shuffled texts (deterministic by r)
            part_idx = 1
            cur_words = 0
            cur_parts = []

            def flush_text():
                nonlocal part_idx, cur_words, cur_parts
                if not cur_parts:
                    return
                
                fname = out_dir / f"part_{part_idx:03d}.json"
                payload = {
                    "title": f"part_{part_idx:03d}",
                    "content": " ".join(cur_parts).strip(),
                    "metadata": {"original_file": "shuffled.none"} 
                }

                fname.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

                part_idx += 1
                cur_words = 0
                cur_parts = []

            print(f"Writing to {out_dir}...")

            for (text,) in conn.execute("SELECT text FROM sents WHERE subpath = ? ORDER BY r", (str(subpath),)):
                wc = self._word_count(text)
                cur_parts.append(text.strip())
                cur_words += wc
                if cur_words >= self.text_target_word_count:
                    flush_text()
            
            flush_text()

            folder_stats[str(subpath)] = {
                "input_sentences": total_sents,
                "unique_sentences": unique_sents,
                "written": part_idx - 1,
                "output_dir": str(out_dir),
            }

        conn.close()
        return folder_stats

    def setLevel(self, level):
        self.level = level
        return self
    
    def setTargetWordCount(self, target_word_count):
        self.text_target_word_count = target_word_count
        return self
    
    def setOutputPath(self, output_path):
        self.output_path = output_path
        return self
    
    def _subpath_from_rel_path(self, rel_path, level):
        """
        Preserve folder structure up to the given level.
        """
        parts = list(Path(rel_path).parts)
        if not parts:
            return Path("(root)")

        folders = parts[:-1] if len(parts) > 1 else [parts[0]]

        if level == 0:
            kept = folders[:1]
        elif level == -1:
            kept = folders
        elif level > 0:
            kept = folders[: min(level + 1, len(folders))]
        else:
            kept = ["(root)"]

        return Path(*kept)
    
    def _norm_sent(self, s):
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def _word_count(self, s: str) -> int:
        # count "word-like" tokens; keeps Romanian diacritics via isalpha check
        cnt = 0
        for tok in s.split():
            if any(ch.isalpha() for ch in tok):
                cnt += 1
        return cnt
    
    def _open_db(self, db_path):
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")  # ~200MB cache 
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sents(
                subpath TEXT NOT NULL,
                h      BLOB NOT NULL,
                r      INTEGER NOT NULL,
                text   TEXT NOT NULL,
                cnt INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(subpath, h)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sents_subpath_r ON sents(subpath, r)")
        return conn
    
    # For database key
    # 16 bit digest is enough
    def _sent_hash(self, s: str):
        return hashlib.blake2b(s.encode("utf-8"), digest_size=16).digest()
    
    # For random order directly from db
    def _sent_randkey(self, h):
        seed_bytes = int(self.seed).to_bytes(8, "little", signed=True)
        y = hashlib.blake2b(h + seed_bytes, digest_size=8).digest()
        return int.from_bytes(y, "little", signed=True)

    def _iter_sent_texts_batch(self, entries_batch, has_spacy: bool):
        """
        Yield sentence texts from a batch of entries.
        Mode is global: either all have .doc or none do.
        """
        if has_spacy:
            for e in entries_batch:
                doc = e.doc
                for sent in doc.sents:
                    yield sent.text
                try:
                    doc.clear()
                except Exception:
                    pass
                del doc
            return

        texts = [getattr(e, "text", "") for e in entries_batch]
        texts = [t for t in texts if t]
        if not texts:
            return

        for doc in self._spacy_model.pipe(texts, batch_size=self.batch_size, n_process=1):
            for sent in doc.sents:
                yield sent.text
            try:
                doc.clear()
            except Exception:
                pass
            del doc

    
    def _is_good_sentence(self, s):
        s = s.strip()

        if len(s) < 10:
            return False, "too_short"

        letters = sum(ch.isalpha() for ch in s)
        if letters < 7:
            return False, "too_few_letters"

        words = [w for w in s.split() if any(ch.isalpha() for ch in w)]
        if len(words) < 2:
            return False, "too_few_words"

        if letters / max(len(s), 1) < 0.50:
            return False, "mostly_non_letters"

        bad_exact = {
            "citește", "citeste",
            "continua", "continuă"
        }
        if self._norm_sent(s) in bad_exact:
            return False, "boilerplate"

        return True, None

