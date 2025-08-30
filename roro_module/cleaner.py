import json
from collections import defaultdict, Counter
from pathlib import Path
import spacy
import gc
import os

class RoRoCleaner:
    def __init__(self, root_path, duplicates_threshold = 0.75, sentence_threshold = 0.25, spacy_model = "ro_core_news_sm", batch_size=512):
        self.root = Path(root_path)
        self.duplicates_threshold = duplicates_threshold
        self.spacy_model = spacy_model
        self.batch_size = batch_size
        self.sentence_threshold = sentence_threshold
        self.nlp = None

    def __load_spacy(self):
        if self.nlp is None:
            print(f"[Cleaner] Loading spaCy model {self.spacy_model} with only tokenizer + sentencizer")
            self.nlp = spacy.load(
                self.spacy_model,
                disable=["tok2vec","morphologizer","parser","attribute_ruler","lemmatizer","ner"]
            )
            if "sentencizer" not in self.nlp.pipe_names:
                self.nlp.add_pipe("sentencizer")
        return self.nlp
    
    def remove_empty(self):
        removed_total = 0
        folder_counts = {}

        for fp in self.root.rglob('*.json'):
            try:
                obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"[skip] {fp}: {e}")
                continue
            content = obj.get("content", "").strip()
            if not content:

                folder = str(fp.parent.relative_to(self.root))
                if not folder:  # top-level files
                    folder = "(root)"
                folder_counts[folder] = folder_counts.get(folder, 0) + 1

                fp.unlink()
                removed_total += 1

        print(f"[Cleaner] Removed {removed_total} empty files from {len(folder_counts)} folders")
        return removed_total, folder_counts
    
    def remove_duplicate_gazetas(self):
        gazeta_to_files = defaultdict(list)
        for fp in self.root.rglob("*.json"):
            gazeta = fp.stem.rsplit("_", 1)[0]
            gazeta_to_files[gazeta].append(fp)

        removed_total = 0
        folder_counts = {}

        for gazeta, files in gazeta_to_files.items():
            contents = []
            for fp in files:
                try:
                    obj = json.loads(fp.read_text(encoding="utf-8"))
                    contents.append(obj.get("content", "").strip())
                except Exception:
                    continue

            if not contents:
                continue

            most_common, count = Counter(contents).most_common(1)[0]
            if count / len(contents) >= self.duplicates_threshold and len(contents) > 10 and len(most_common) > 100:
                # remove entire gazeta
                for fp in files:
                    folder = str(fp.parent.relative_to(self.root))
                    if not folder:
                        folder = "(root)"
                    folder_counts[folder] = folder_counts.get(folder, 0) + 1
                    fp.unlink()
                removed_total += len(files)
                print(f"[Cleaner] Removed {gazeta}")

        print(f"[Cleaner] Removed {removed_total} files from {len(folder_counts)} folders (duplicate gazetas)")
        return removed_total, folder_counts
    
    def flag_duplicate_sentences(self):
        nlp = self.__load_spacy()
        gazeta_to_files = defaultdict(list)

        # group files by gazeta name
        for fp in self.root.rglob("*.json"):
            gazeta = fp.stem.rsplit("_", 1)[0]
            gazeta_to_files[gazeta].append(fp)

        flagged_gazetas = {}
        for gazeta, files in gazeta_to_files.items():
            entry_count = len(files)
            sentence_to_entries = defaultdict(set)  # sentence → set of file paths

            for doc, fp in zip(
            nlp.pipe((json.loads(fp.read_text(encoding="utf-8")).get("content", "") for fp in files),
                     batch_size=self.batch_size),
            files
            ):
                for sent in doc.sents:
                    s = self._normalize_sentence(sent.text)
                    if s:
                        sentence_to_entries[s].add(fp)
                del doc
            print(f"Processed gazeta {gazeta} with {entry_count} entries")
            gc.collect()

            # find if any sentence appears in ≥25% of entries
            bad_sentences = []
            for s, entry_set in sentence_to_entries.items():
                ratio = len(entry_set) / entry_count
                if ratio >= self.sentence_threshold:
                    bad_sentences.append((s, ratio))

            if bad_sentences:
                flagged_gazetas[gazeta] = {
                    "files": entry_count,
                    "flagged_sentences": bad_sentences
                }

        print(f"[Cleaner] Flagged {len(flagged_gazetas)} gazetas with more than {self.sentence_threshold * 100}% duplicate sentences across entries")
        return flagged_gazetas
    
    def _normalize_sentence(self, s):
        import re, unicodedata
        s = unicodedata.normalize("NFKC", s)
        s = s.lower().strip()
        s = re.sub(r"\s+", " ", s)
        return s
