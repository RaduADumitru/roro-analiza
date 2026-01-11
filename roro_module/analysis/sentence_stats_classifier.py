from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score, balanced_accuracy_score, matthews_corrcoef, confusion_matrix

import spacy 

@dataclass
class LogRegConfig:
    C = 1.0
    max_iter = 1000
    solver = "saga"  # "liblinear" or "saga"
    class_weight = "balanced"

class RoRoSentenceStatsClassifier:

    def __init__(
        self,
        level = -1,
        test_size = 0.2,
        random_state = 42,
        logreg = LogRegConfig(),
        spacy_model = "ro_core_news_sm",
        batch_size = 128
    ):
        self.level = level
        self.logreg_cfg = logreg
        self.test_size = test_size
        self.random_state = random_state
        self.pipeline = None
        self.label_order_ = None
        self._spacy_model_name = spacy_model
        self._spacy_model = None
        self.batch_size = batch_size

    def _folder_from_rel_path(self, rel_path, level):
        """
        Given a relative path and a level, return the corresponding folder name.

        The level is interpreted as follows:
        - 0: root folder
        - -1: last folder
        - > 0: specific folder depth

        If the level is invalid or the relative path is empty, return "(root)".
        """
        parts = list(Path(rel_path).parts)
        if not parts:
            return "(root)"

        if level == 0:
            folder = parts[0]
        elif level == -1:
            folder = parts[-2] if len(parts) > 1 else parts[0]
        elif level > 0:
            if level < len(parts) - 1:
                folder = parts[level]
            else:
                folder = parts[-2] if len(parts) > 1 else parts[0]
        else:
            folder = "(root)"
        return folder

    def _extract_xy(self, entries):
        """
        Extract the X and y values from the given entries.

        X is a list of text strings, while y is a list of the corresponding
        folder names. The label_counts dictionary contains the number of
        occurrences of each folder name.

        The folder name is determined by the rel_path attribute of each entry,
        combined with the level attribute of the current object. The level
        attribute is interpreted as follows:

        - 0: root folder
        - -1: last folder
        - > 0: specific folder depth

        If the rel_path attribute is empty, the folder name is set to "(root)".

        :param entries: a list of objects with text and rel_path attributes
        :return: X, y, and label_counts
        """
        X, Xdocs, y = [], [], []
        label_counts = defaultdict(int)

        for e in entries:
            # Prefer doc.text if available, else .text
            doc = getattr(e, "doc", None)
            text = doc.text if doc is not None else getattr(e, "text", None)
            if not text:
                continue

            rel_path = e.meta.get("rel_path", "")
            folder = self._folder_from_rel_path(rel_path, self.level)

            X.append(text)
            Xdocs.append(doc)
            y.append(folder)
            label_counts[folder] += 1

        return X, Xdocs, y, dict(label_counts)
    
    def _chunks(self, seq, size):
        """Yield successive slices of size from seq."""
        for i in range(0, len(seq), size):
            yield seq[i:i + size]

    def _stats_from_doc(self, doc):
        
        sent_lengths = []
        punct_counts = []
        stop_counts = []
        pron_counts = []

        for sent in doc.sents:
            words = [t for t in sent if t.is_alpha]
            puncts = [t for t in sent if t.is_punct]
            stops = [t for t in sent if t.is_stop]
            prons = [t for t in sent if t.pos_ == "PRON"]

            if words or puncts or stops or prons:
                sent_lengths.append(len(words))
                punct_counts.append(len(puncts))
                stop_counts.append(len(stops))
                pron_counts.append(len(prons))

        avg_sent_len = sum(sent_lengths) / len(sent_lengths) if sent_lengths else 0.0
        avg_punct = sum(punct_counts) / len(punct_counts) if punct_counts else 0.0
        avg_stop = sum(stop_counts) / len(stop_counts) if stop_counts else 0.0
        avg_pron = sum(pron_counts) / len(pron_counts) if pron_counts else 0.0

        # unique word percentage (per doc)
        words_in_doc = [t.text.lower() for t in doc if t.is_alpha]
        unique_pct = (len(set(words_in_doc)) / len(words_in_doc)) if words_in_doc else 0.0

        return avg_sent_len, avg_punct, avg_stop, avg_pron, unique_pct
    
    def _from_text(self, data, which = "Training"):
         
        return_data = []

        total_processed = 0
        for chunk in self._chunks(data, self.batch_size):
            for entry, doc in zip(chunk, self._spacy_model.pipe(chunk, batch_size=self.batch_size, n_process=-1)):
                return_data.append(self._stats_from_doc(doc))

                total_processed += 1

                del doc

            print(f"{which} data processed: {total_processed}/{len(data)} ({total_processed / len(data) * 100:.2f}%)")    

        return return_data

    def run(self, entries, **kwargs):
       
        level = kwargs.get("level", self.level)
        self.level = level

        verbose = kwargs.get("verbose", False)

        self._spacy_model_name = kwargs.get("spacy_model_name", self._spacy_model_name)

        X, Xdocs, y, label_counts = self._extract_xy(entries)

        if len(set(y)) < 2:
            return {
                "error": "Need at least two distinct labels. "
                         f"Found labels: {sorted(set(y))}",
                "label_counts": label_counts
            }
        
        self.labels_order_ = list(set(y))

        X_train, X_test, Xdocs_train, Xdocs_test, y_train, y_test = train_test_split(
            X, Xdocs, y, test_size=self.test_size, random_state=self.random_state, stratify=y
        )


        # Check if data has spacy already 
        if Xdocs_train[0] is not None:
            X_train_data = np.asarray(
                [self._stats_from_doc(doc) for doc in Xdocs_train],
                dtype=np.float32
            )
            print("Training: processed data with spacy")
            X_test_data = np.asarray(
                [self._stats_from_doc(doc) for doc in Xdocs_test],
                dtype=np.float32
            )
            print("Test: processed data with spacy")
        else:
            self._spacy_model = spacy.load(self._spacy_model_name)
            
            X_train_data = self._from_text(X_train)
            X_test_data = self._from_text(X_test, "Test")

        self.clf = LogisticRegression(
            C=self.logreg_cfg.C,
            max_iter=self.logreg_cfg.max_iter,
            solver=self.logreg_cfg.solver,
            class_weight=self.logreg_cfg.class_weight,
            verbose=verbose
        )
        self.clf.fit(X_train_data, y_train)

        y_pred = self.clf.predict(X_test_data)
        
        acc = accuracy_score(y_test, y_pred)
        acc_bal = balanced_accuracy_score(y_test, y_pred)
        mcc = matthews_corrcoef(y_test, y_pred)

        cm_norm = confusion_matrix(y_test, y_pred, labels=self.labels_order_, normalize='true')
        cm = confusion_matrix(y_test, y_pred, labels=self.labels_order_)

        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        

        return {'stats':{'result':{
                "processed": len(X),
                "level_used": self.level,
                "accuracy": acc,
                "balanced_accuracy": acc_bal,
                "mcc": mcc,
            }},
            'data': {
                "classification_report": report,
                "label_counts": label_counts,
                "labels": self.label_order_,
            },
            'matrix':
            {
                "labels": self.labels_order_,
                "confusion_matrix": cm.tolist(),
                "confusion_matrix_norm": cm_norm.tolist()
            }
        }
