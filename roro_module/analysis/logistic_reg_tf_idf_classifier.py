from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score, balanced_accuracy_score, matthews_corrcoef, confusion_matrix

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

        self._doc_cache = {}

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

    def _extract_xy(self, entries, only_functional = False):
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

            if only_functional:
                # Reuse doc if available, otherwise create it once here
                if doc is None:
                    if self._spacy_model is None:
                        self._spacy_model = spacy.load(self._spacy_model_name)
                    doc = self._spacy_model(text)

                tokens = self._functional_tokens_from_doc(doc)
                text = " ".join(tokens)
            

            rel_path = e.meta.get("rel_path", "")
            folder = self._folder_from_rel_path(rel_path, self.level)

            X.append(text)
            y.append(folder)
            label_counts[folder] += 1

        return X, y, dict(label_counts)
    
    def _functional_tokens_from_doc(self, doc):
        """
        Given a spaCy Doc, return a list of functional-word tokens (lowercased).
        """
        return [
            tok.text.lower()
            for tok in doc
            if not tok.is_space
            and not tok.is_punct
            and tok.pos_ in {"ADP", "CCONJ", "SCONJ", "PRON", "DET", "AUX", "PART", "INTJ"}
        ]


    def _build_pipeline(self, only_functional = False, verbose = False):
        """
        Build a scikit-learn Pipeline consisting of a TfidfVectorizer and a
        LogisticRegression classifier. The hyperparameters are set according to
        the attributes of self.tfidf_cfg and self.logreg_cfg.

        :return: a Pipeline object
        """

        self.vectorizer = TfidfVectorizer(
            analyzer=self.tfidf_cfg.analyzer,
            ngram_range=self.tfidf_cfg.ngram_range if not only_functional else (1, 1),
            max_df=self.tfidf_cfg.max_df,
            min_df=self.tfidf_cfg.min_df,
            max_features=self.tfidf_cfg.max_features,
            sublinear_tf=self.tfidf_cfg.sublinear_tf,
            lowercase=self.tfidf_cfg.lowercase if not only_functional else False,
            strip_accents=self.tfidf_cfg.strip_accents,
            token_pattern=r"(?u)\b\w\w+\b"
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

    def _top_features(self, k = 10):
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
    
    def _prepare_common(self, entries, **kwargs):
        level = kwargs.get("level", self.level)
        self.level = level

        only_functional = kwargs.get("only_functional", False)
        verbose = kwargs.get("verbose", False)

        X, y, label_counts = self._extract_xy(entries, only_functional)

        if len(set(y)) < 2:
            return {"error": "Need at least two distinct labels.", "label_counts": label_counts}
        
        self.label_order_ = list(set(y))

        ctx = {
            "X": X,
            "y": np.array(y),
            "label_counts": label_counts,
            "only_functional": only_functional,
            "verbose": verbose
        }

        return ctx

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
        

        cv_folds = kwargs.get("cv_folds", None)

        if cv_folds is None or cv_folds < 2:
            return self._run_single_split(entries, **kwargs)
        
        else:
            return self._run_cross_validation(entries, **kwargs)

    def _run_single_split(self, entries, **kwargs):
        ctx = self._prepare_common(entries, **kwargs)

        if "error" in ctx:
            return ctx
        
        X              = ctx["X"]
        y              = ctx["y"]
        label_counts   = ctx["label_counts"]
        only_functional = ctx["only_functional"]
        verbose        = ctx["verbose"]


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
        
        acc = accuracy_score(y_test, y_pred)
        acc_bal = balanced_accuracy_score(y_test, y_pred)
        mcc = matthews_corrcoef(y_test, y_pred)

        cm_norm = confusion_matrix(y_test, y_pred, labels=self.label_order_, normalize='true')
        cm = confusion_matrix(y_test, y_pred, labels=self.label_order_)

        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        


        # Save fitted parts for later reuse
        self.pipeline = pipe
        self.vectorizer = pipe.named_steps["tfidf"]
        self.clf = pipe.named_steps["clf"]

        top_feats = self._top_features(k=10)
        # Implode the feature lists into strings
        top_feats_str = {k: ", ".join(v) for k, v in top_feats.items()}

        return {'stats':{'result':{
                "processed": len(X),
                "level_used": self.level,
                "accuracy": acc,
                "balanced_accuracy": acc_bal,
                "mcc": mcc,
                "roc_auc": roc_auc,
                **top_feats_str
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
    

    def _run_cross_validation(self, entries, **kwargs):
        ctx = self._prepare_common(entries, **kwargs)

        if "error" in ctx:
            return ctx
        
        cv_folds       = kwargs.get("cv_folds")
        X              = ctx["X"]
        y              = ctx["y"]
        label_counts   = ctx["label_counts"]
        only_functional = ctx["only_functional"]
        verbose        = ctx["verbose"]

        skf = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=self.random_state)

        all_y_test = []
        all_y_pred = []
        all_probs = [] if len(self.label_order_) == 2 else None
        fold_metrics = []

        for fold_idx, (train_idx, test_idx) in enumerate(skf.split(X, y), start = 1):
            X_train = [X[i] for i in train_idx]
            X_test = [X[i] for i in test_idx]
            y_train = [y[i] for i in train_idx]
            y_test = [y[i] for i in test_idx]

            pipe = self._build_pipeline(only_functional, verbose)
            pipe.fit(X_train, y_train)

            y_pred = pipe.predict(X_test)
            
            all_y_test.append(y_test)
            all_y_pred.append(y_pred)

            roc_auc_fold = None

            if hasattr(pipe.named_steps["clf"], "predict_proba") and len(self.label_order_) == 2:
                y_proba = pipe.predict_proba(X_test)[:, 1]
                if all_probs is not None:
                    all_probs.append(y_proba)
                try:    
                    roc_auc_fold = roc_auc_score(y_test, y_proba)
                except Exception:
                    pass
            
            acc = accuracy_score(y_test, y_pred)
            acc_bal = balanced_accuracy_score(y_test, y_pred)
            mcc = matthews_corrcoef(y_test, y_pred)

            fold_metrics.append({
                "fold": fold_idx,
                "n_test": int(len(y_test)),
                "accuracy": acc,
                "balanced_accuracy": acc_bal,
                "mcc": mcc,
                "roc_auc": roc_auc_fold,
            })

            if verbose:
                print(f"Fold {fold_idx} accuracy: {acc}")
                print(f"Fold {fold_idx} balanced accuracy: {acc_bal}")
                print(f"Fold {fold_idx} MCC: {mcc}")
                print(f"Fold {fold_idx} ROC-AUC: {roc_auc_fold}")
                print(f"Fold {fold_idx} n_test: {int(len(y_test))}")

        # Aggregation

        y_test_all = np.concatenate(all_y_test)
        y_pred_all = np.concatenate(all_y_pred)

        cm = confusion_matrix(y_test_all, y_pred_all, labels=self.label_order_)
        cm_norm = confusion_matrix(y_test_all, y_pred_all, labels=self.label_order_, normalize='true')

        report = classification_report(y_test_all, y_pred_all, output_dict=True, zero_division=0)

        accs = [f["accuracy"] for f in fold_metrics]
        acc_bals = [f["balanced_accuracy"] for f in fold_metrics]
        mccs = [f["mcc"] for f in fold_metrics]
        roc_aucs = [f["roc_auc"] for f in fold_metrics if f["roc_auc"] is not None]

        roc_auc_global = None
        if all_probs is not None and len(all_probs) > 0:
            try:
                probs_all = np.concatenate(all_probs)
                roc_auc_global = roc_auc_score(y_test_all, probs_all)
            except Exception:
                roc_auc_global = None

        # Fit on full data once for top features / final model
        pipe_full = self._build_pipeline(only_functional, verbose)
        pipe_full.fit(X, y)
        self.pipeline = pipe_full
        self.vectorizer = pipe_full.named_steps["tfidf"]
        self.clf = pipe_full.named_steps["clf"]

        top_feats = self._top_features(k = 10)
        top_feats_str = {k: ", ".join(v) for k, v in top_feats.items()}


        stats = {}

        # One row per fold
        for m in fold_metrics:
            fold_key = f"fold_{m['fold']}"
            stats[fold_key] = {
                "fold": m["fold"],
                "n_test": m["n_test"],
                "accuracy": m["accuracy"],
                "balanced_accuracy": m["balanced_accuracy"],
                "mcc": m["mcc"],
                "roc_auc": m["roc_auc"],
            }

        # Aggregate row
        stats["aggregate"] = {
            "processed": len(X),
            "level_used": self.level,
            "accuracy_mean": float(np.mean(accs)),
            "accuracy_std": float(np.std(accs)),
            "balanced_accuracy_mean": float(np.mean(acc_bals)),
            "balanced_accuracy_std": float(np.std(acc_bals)),
            "mcc_mean": float(np.mean(mccs)),
            "mcc_std": float(np.std(mccs)),
            "roc_auc_mean": float(np.mean(roc_aucs)) if roc_aucs else None,
            "roc_auc_global": roc_auc_global,
            "cv_folds": cv_folds,
            **top_feats_str,
        }

        return {
            "stats": stats,
            "data": {
                "classification_report": report,
                "label_counts": label_counts,
                "model": self.clf,
                "vectorizer": self.vectorizer,
                "labels": self.label_order_,
            },
            "matrix": {
                "labels": self.label_order_,
                "confusion_matrix": cm.tolist(),
                "confusion_matrix_norm": cm_norm.tolist(),
            }
        }
