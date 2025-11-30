from dataclasses import dataclass
from pathlib import Path
from collections import defaultdict
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, accuracy_score, balanced_accuracy_score, matthews_corrcoef, roc_auc_score
from sklearn.metrics import confusion_matrix
from sklearn.base import TransformerMixin, BaseEstimator

import torch

from transformers import (
    AutoTokenizer, AutoModel
)

@dataclass
class LogRegConfig:
    C = 1.0
    max_iter = 1000
    solver = "saga"  # "liblinear" or "saga"
    class_weight = "balanced"

class BertEmbedder(BaseEstimator, TransformerMixin):
    def __init__(self, model_name, verbose = False, batch_size = 128):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.verbose = verbose
        self.batch_size = batch_size
        self.model_name = model_name

        print(f"Is verbose? {self.verbose}")

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.model.eval()

    def transform(self, texts):
        all_vecs = []
        n = len(texts)

        for start in range(0, n, self.batch_size):
            end = start + self.batch_size
            if end > n:
                end = n

            proc = start / n * 100
            batch_texts = texts[start:end]

            if self.verbose:
                print(f"Embedding {start} - {end} / {n} ({proc:.2f}%)")

            enc = self.tokenizer(
                batch_texts,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512,
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}

            with torch.no_grad():
                out = self.model(**enc).last_hidden_state[:, 0, :]  # CLS

            all_vecs.append(out.cpu().numpy())

        return np.vstack(all_vecs)

    def fit(self, X, y=None):
        return self

class RoRoBertLogisticRegressionClassifier:

    def __init__(
        self,
        level = -1,
        test_size = 0.2,
        random_state = 42,
        logreg = LogRegConfig(),
        bert_model = "dumitrescustefan/bert-base-romanian-cased-v1"
    ):
        self.level = level
        self.logreg_cfg = logreg
        self.test_size = test_size
        self.random_state = random_state
        self.pipeline = None
        self.vectorizer = None
        self.clf = None
        self.label_order_ = None
        self.bert_model = bert_model

    def _folder_from_rel_path(self, rel_path, level):
        parts = list(Path(rel_path).parts)
        if not parts: return "(root)"
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
        X, y = [], []
        label_counts = defaultdict(int)
        for e in entries:
            doc = getattr(e, "doc", None)
            text = doc.text if doc is not None else getattr(e, "text", None)
            if not text:
                continue
            rel_path = e.meta.get("rel_path", "")
            folder = self._folder_from_rel_path(rel_path, self.level)
            X.append(text); y.append(folder)
            label_counts[folder] += 1
        return X, y, dict(label_counts)
    
    def _build_pipeline(self, verbose = False):
        self.embedder = BertEmbedder(self.bert_model, verbose)

        self.clf = LogisticRegression(
            C=self.logreg_cfg.C,
            max_iter=self.logreg_cfg.max_iter,
            solver=self.logreg_cfg.solver,
            class_weight=self.logreg_cfg.class_weight,
            verbose=verbose
        )
        return Pipeline([
            ("bert", self.embedder),
            ("clf", self.clf),
     ])

    def run(self, entries, **kwargs):

        # Allow overrides via analyzer.run kwargs
        self.level      = kwargs.get("level", self.level)
        self.bert_model      = kwargs.get("bert_name",   self.bert_model)

        random_state = kwargs.get("random_state", 42)
        test_size = kwargs.get("test_size", 0.2)
        verbose = kwargs.get("verbose", False)


        X, y, label_counts = self._extract_xy(entries)
        if len(set(y)) < 2:
            return {"error": "Need at least two distinct labels.", "label_counts": label_counts}

        print (f"Prepared {len(X)} entries with {len(set(y))} labels")
        self.label_order_ = list(set(y))

        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )

        print(f"Training on {len(X_train)} entries with {len(set(y_train))} labels")

        pipe = self._build_pipeline(verbose)

        print("Built pipeline")
        pipe.fit(X_train, y_train)
        print("Fit pipeline")

        y_pred = pipe.predict(X_test)
        y_proba = None
        roc_auc = None

        # ROC-AUC only meaningful for binary with predict_proba
        if hasattr(pipe.named_steps["clf"], "predict_proba") and len(set(y)) == 2:
            y_proba = pipe.predict_proba(X_test)[:, 1]
            # Map positive class index
            # classes_[1] is the positive class for the proba used above
            roc_auc = roc_auc_score(y_test, y_proba)
        
        acc = accuracy_score(y_test, y_pred)
        acc_bal = balanced_accuracy_score(y_test, y_pred)
        mcc = matthews_corrcoef(y_test, y_pred)

        cm_norm = confusion_matrix(y_test, y_pred, labels=self.label_order_, normalize='true')
        cm = confusion_matrix(y_test, y_pred, labels=self.label_order_)

        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)

        # Save fitted parts for later reuse
        self.pipeline = pipe
        self.vectorizer = pipe.named_steps["bert"]
        self.clf = pipe.named_steps["clf"]

        return {'stats':{'result':{
                "processed": len(X),
                "level_used": self.level,
                "accuracy": acc,
                "balanced_accuracy": acc_bal,
                "mcc": mcc,
                "roc_auc": roc_auc,
            }},
            'data': {
                "classification_report": report,
                "label_counts": label_counts,
                "model": self.clf,
                "vectorizer": self.vectorizer,
                "labels": self.label_order_,
            },
            'matrix':
            {
                "labels": self.label_order_,
                "confusion_matrix": cm.tolist(),
                "confusion_matrix_norm": cm_norm.tolist()
            }
        }
