
from pathlib import Path
import random
from collections import defaultdict
import spacy
import re 
import json

""" 
Class RoRoShuffler
 - Loads data from the Parser class
 - For each requested level,
  -- splits text into sentences, 
  -- removes duplicates,
  -- shuffles them
  -- recombines them into texts with target number of words.
"""

class RoRoShuffler:
    def __init__(self, parser, **kwargs):
        self.parser = parser

        self.level = kwargs.get("level", -1)
        self.text_target_word_count = kwargs.get("target_word_count", 2000)
        self.output_path = kwargs.get("output_path", "shuffler_output") 

        self.spacy_model_name = kwargs.get("spacy_model", "ro_core_news_sm")
        self.batch_size = kwargs.get("batch_size", 512)
        self.seed = kwargs.get("seed", 42)

        self._spacy_model = None

    def run(self):

        rng = random.Random(self.seed)
        entries = self.parser.get_flat()
        out_root = Path(self.output_path)
        out_root.mkdir(parents=True, exist_ok=True)

        by_folder_content = defaultdict(list)
        has_spacy = False

        for e in entries:

            rel_path = e.meta.get("rel_path", "")
            subpath = self._subpath_from_rel_path(rel_path, self.level)

            doc = getattr(e, "doc", None)
            if doc is not None:
                has_spacy = True
                by_folder_content[subpath].append(doc)

            else:
                text = getattr(e, "text", "")
                by_folder_content[subpath].append(text)

        if not has_spacy:
            self._spacy_model = spacy.load(self.spacy_model_name, disable=["ner", "lemmatizer", "textcat"])
            if "parser" not in self._spacy_model.pipe_names and "senter" not in self._spacy_model.pipe_names and "sentencizer" not in self._spacy_model.pipe_names:
                self._spacy_model.add_pipe("sentencizer")

        total_written = 0
        folder_stats = {}

        all_folders = sorted(set(by_folder_content.keys()))

        for subpath in all_folders:

            print (f"Shuffling {subpath}")
            sentences = []

            # Grab all sentences from text
            if has_spacy:
                for doc in by_folder_content.get(subpath, []):
                    sentences.extend(self._sentences_from_doc(doc))

            else:
                texts = by_folder_content.get(subpath, [])
                if texts:
                    for doc in self._spacy_model.pipe(texts, batch_size=self.batch_size, n_process=-1):
                        sentences.extend(self._sentences_from_doc(doc))
            

            # Remove duplicates, after normalization
            unique_sentences = []
            seen_sentences = set()
            for s in sentences:
                key = self._norm_sent(s)
                if not key or key in seen_sentences:
                    continue
                seen_sentences.add(key)
                unique_sentences.append(s.strip())

            # Shuffle
            rng.shuffle(unique_sentences)

            # Recombine into text with target word count
            out_dir = out_root / subpath
            out_dir.mkdir(parents=True, exist_ok=True)

            print (f"Writing {len(unique_sentences)} sentences to {out_dir}...")
            written_this_folder = 0
            for idx, text in enumerate(self._make_texts_close_to_target(unique_sentences), start = 1):
                fname = out_dir / f"part_{idx:03d}.txt"

                payload = {
                    "title": f"part_{idx:03d}",
                    "content": text.strip(),
                    "metadata": {
                        "original_file": "shuffled.none"
                    }
                }
                fname.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
                total_written += 1
                written_this_folder += 1

            folder_stats[subpath] = {
                "input": len(sentences),
                "unique": len(unique_sentences),
                "written": written_this_folder,
                "output_dir": out_dir
            }

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
    

    def _sentences_from_doc(self, doc):
        # safe if pipeline has sentencizer/parser; otherwise try fallback
        if getattr(doc, "sents", None) is not None:
            sents = [s.text for s in doc.sents]
            if sents:
                return sents
            
        raise Exception("Could not get sentences from doc")

    def _norm_sent(self, s):
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def _make_texts_close_to_target(self, sentences):
        """
        Add sentences until above target, then keep whichever (before/after)
        is closer to target. The rejected sentence becomes the start of next text.
        """
        i = 0
        n = len(sentences)

        while i < n:
            cur = []
            cur_wc = 0

            # skip empties
            while i < n and not sentences[i].strip():
                i += 1
            if i >= n:
                break

            while i < n:
                s = sentences[i].strip()
                if not s:
                    i += 1
                    continue

                s_wc = self._word_count(s)
                before_wc = cur_wc
                after_wc = cur_wc + s_wc

                # if adding doesn't reach target, add and continue
                if after_wc < self.text_target_word_count:
                    cur.append(s)
                    cur_wc = after_wc
                    i += 1
                    continue

                # we reached/passed target; decide closer
                before_diff = abs(self.text_target_word_count - before_wc)
                after_diff = abs(self.text_target_word_count - after_wc)

                if after_diff <= before_diff:
                    cur.append(s)
                    cur_wc = after_wc
                    i += 1
                # else: reject s for next text (do not increment i)

                break

            if cur:
                yield " ".join(cur)
            else:
                # pathological case: first sentence is empty/0 words -> skip it
                i += 1

    def _word_count(self, s: str) -> int:
        # count "word-like" tokens; keeps Romanian diacritics via isalpha check
        cnt = 0
        for tok in s.split():
            if any(ch.isalpha() for ch in tok):
                cnt += 1
        return cnt