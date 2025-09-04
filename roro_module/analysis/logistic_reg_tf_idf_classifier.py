from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score

import spacy 

@dataclass
class TfIdfConfig:
    analyzer = "word"
    ngram_range = (1, 2)
    max_df = 0.85
    min_df = 5
    max_features = 20000
    sublinear_tf = True
    lowercase = True
    strip_accents = "unicode"

@dataclass
class LogRegConfig:
    C = 1.0
    max_iter = 1000
    solver = "saga"  # "liblinear" or "saga"
    class_weight = "balanced"

class RoRoLogisticRegTfIdfClassifier:

    def __init__(
        self,
        level = -1,
        test_size = 0.2,
        random_state = 42,
        tfidf = TfIdfConfig(),
        logreg = LogRegConfig(),
        spacy_model = "ro_core_news_sm"
    ):
        self.level = level
        self.tfidf_cfg = tfidf
        self.logreg_cfg = logreg
        self.test_size = test_size
        self.random_state = random_state
        self.pipeline = None
        self.vectorizer = None
        self.clf = None
        self.label_order_ = None
        self._spacy_model_name = spacy_model
        self._spacy_model = None

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
        X, y = [], []
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
            y.append(folder)
            label_counts[folder] += 1

        return X, y, dict(label_counts)
    
    def _function_word_analyzer (self, text):

        if not self._spacy_model:
            self._spacy_model = spacy.load(self._spacy_model_name)
        
        doc = self._spacy_model(text) 
        for tok in doc:
            if tok.is_space or tok.is_punct:
                continue
            if tok.pos_ in {"ADP","CCONJ","SCONJ","PRON","DET","AUX","PART","INTJ"}:
                yield tok.text.lower()


    def _build_pipeline(self, only_functional = False, verbose = False):
        """
        Build a scikit-learn Pipeline consisting of a TfidfVectorizer and a
        LogisticRegression classifier. The hyperparameters are set according to
        the attributes of self.tfidf_cfg and self.logreg_cfg.

        :return: a Pipeline object
        """

        analyzer = self.tfidf_cfg.analyzer
        if only_functional:
            analyzer = self._function_word_analyzer

        self.vectorizer = TfidfVectorizer(
            analyzer=analyzer,
            ngram_range=self.tfidf_cfg.ngram_range if not only_functional else (1, 1),
            max_df=self.tfidf_cfg.max_df,
            min_df=self.tfidf_cfg.min_df,
            max_features=self.tfidf_cfg.max_features,
            sublinear_tf=self.tfidf_cfg.sublinear_tf,
            lowercase=self.tfidf_cfg.lowercase if not only_functional else False,
            strip_accents=self.tfidf_cfg.strip_accents,
            token_pattern=r"(?u)\b\w\w+\b" if not only_functional else None
        )
        self.clf = LogisticRegression(
            C=self.logreg_cfg.C,
            max_iter=self.logreg_cfg.max_iter,
            solver=self.logreg_cfg.solver,
            class_weight=self.logreg_cfg.class_weight,
            verbose=verbose
        )
        return Pipeline([
            ("tfidf", self.vectorizer),
            ("clf", self.clf),
        ])

    def _top_features(self, k = 20):
        """
        Return a dictionary with the top k features for each class.

        The returned dictionary will have the class names as keys, and the
        corresponding values will be lists of the top k feature names for
        each class, sorted in descending order of importance.

        :param k: the number of top features to return per class
        :return: a dictionary with class names as keys and lists of feature names as values
        """
        if self.vectorizer is None or self.clf is None:
            return {}

        feature_names = np.array(self.vectorizer.get_feature_names_out())
        # Binary LR -> coef_ shape (1, n_features), multi-class -> (n_classes, n_features)
        coef = self.clf.coef_
        classes = self.clf.classes_
        self.label_order_ = list(classes)

        out = {}
        if coef.shape[0] == 1:
            # Binary: positive class = classes[1]
            pos_idx = np.argsort(coef[0])[-k:]
            neg_idx = np.argsort(coef[0])[:k]
            out[classes[1]] = feature_names[pos_idx].tolist()
            out[classes[0]] = feature_names[neg_idx].tolist()
        else:
            # One-vs-rest: per class
            for i, c in enumerate(classes):
                top_idx = np.argsort(coef[i])[-k:]
                out[c] = feature_names[top_idx].tolist()
        return out

    def run(self, entries, **kwargs):
        """
        Run a logistic regression with TF-IDF features on the given entries.

        This will first extract the relevant text data from the entries,
        then split it into a training and test set. It will then train a
        logistic regression model on the training set and evaluate it on
        the test set. The results will include the classification report,
        accuracy, and optionally the ROC-AUC score. The top 20 features
        per class will also be returned.

        :param entries: a list of entries to be processed
        :param level: the level to use for the text extraction
        :param kwargs: additional keyword arguments to be passed to _extract_xy
        :return: a dictionary with the results, including the classification report,
            accuracy, and optionally the ROC-AUC score, and the top features per class
        """
        level = kwargs.get("level", self.level)
        self.level = level

        only_functional = kwargs.get("only_functional", False)

        verbose = kwargs.get("verbose", False)

        X, y, label_counts = self._extract_xy(entries)

        if len(set(y)) < 2:
            return {
                "error": "Need at least two distinct labels. "
                         f"Found labels: {sorted(set(y))}",
                "label_counts": label_counts
            }

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.random_state, stratify=y
        )

        pipe = self._build_pipeline(only_functional, verbose)
        pipe.fit(X_train, y_train)

        y_pred = pipe.predict(X_test)
        y_proba = None
        roc_auc = None

        # ROC-AUC only meaningful for binary with predict_proba
        if hasattr(pipe.named_steps["clf"], "predict_proba") and len(set(y)) == 2:
            y_proba = pipe.predict_proba(X_test)[:, 1]
            # Map positive class index
            # classes_[1] is the positive class for the proba used above
            roc_auc = roc_auc_score(y_test, y_proba)

        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        acc = accuracy_score(y_test, y_pred)

        # Save fitted parts for later reuse
        self.pipeline = pipe
        self.vectorizer = pipe.named_steps["tfidf"]
        self.clf = pipe.named_steps["clf"]

        top_feats = self._top_features(k=20)
        # Implode the feature lists into strings
        top_feats_str = {k: ", ".join(v) for k, v in top_feats.items()}

        return {'stats':{'result':{
                "processed": len(X),
                "level_used": self.level,
                "accuracy": acc,
                "roc_auc": roc_auc,
                **top_feats_str
            }},
            'data': {
                "classification_report": report,
                "label_counts": label_counts,
                # Optionally return the trained objects
                "model": self.clf,
                "vectorizer": self.vectorizer,
                "labels": self.label_order_,
            }
        }
