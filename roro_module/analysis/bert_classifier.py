from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import os, psutil
import numpy as np

import spacy

from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score, balanced_accuracy_score, matthews_corrcoef, roc_auc_score
from sklearn.metrics import confusion_matrix



import torch
from torch.utils.data import Dataset

from transformers import (
    AutoTokenizer, AutoModelForSequenceClassification,
    DataCollatorWithPadding, TrainingArguments, Trainer, EarlyStoppingCallback
)
import evaluate


@dataclass
class BertConfig:
    model_name = "dumitrescustefan/bert-base-romanian-cased-v1"
    max_length = 256
    batch_size = 16
    num_epochs = 3
    lr: float = 2e-5
    weight_decay = 0.01
    fp16 = True            # set False on CPU
    logging_steps = 50
    output_dir = "./roro_bert_out"


class _SimpleTextDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_length=256):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_length = max_length
    def __len__(self): return len(self.texts)
    def __getitem__(self, idx):
        enc = self.tokenizer(
            self.texts[idx],
            truncation=True,
            max_length=self.max_length
        )
        enc["labels"] = int(self.labels[idx])
        return enc


class RoRoBertClassifier:

    def __init__(self, level=-1, bert_cfg: BertConfig = BertConfig()):
            
        self.level = level
        self.cfg = bert_cfg
        self.tokenizer = None
        self.model = None
        self.label_encoder = None
        self.label_order_ = None

        self._spacy_model_name ="ro_core_news_lg"
        self._spacy_model = None

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

    def _extract_xy(self, entries, only_functional = False):
        X, y = [], []
        label_counts = defaultdict(int)
        for e in entries:
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
            X.append(text); y.append(folder)
            label_counts[folder] += 1
        return X, y, dict(label_counts)
    
    def run(self, entries, **kwargs):
        cv_folds = kwargs.get("cv_folds", None)

        if cv_folds is None or cv_folds < 2:
            return self._run_single_split(entries, **kwargs)
        else:
            return self._run_cross_validation(entries, **kwargs)
        
    def _prepare_common(self, entries, **kwargs): 
        self.level      = kwargs.get("level", self.level)
        model_name      = kwargs.get("model_name",   self.cfg.model_name)
        max_length      = kwargs.get("max_length",   self.cfg.max_length)
        batch_size      = kwargs.get("batch_size",   self.cfg.batch_size)
        num_epochs      = kwargs.get("num_epochs",   self.cfg.num_epochs)
        lr              = kwargs.get("lr",           self.cfg.lr)
        weight_decay    = kwargs.get("weight_decay", self.cfg.weight_decay)
        fp16            = kwargs.get("fp16",         self.cfg.fp16 and torch.cuda.is_available())
        logging_steps   = kwargs.get("logging_steps",self.cfg.logging_steps)
        output_dir      = kwargs.get("output_dir",   self.cfg.output_dir)
        freeze_encoder  = kwargs.get("freeze_encoder", False) # optional speedup on CPU
        only_functional = kwargs.get("only_functional", False)
        spacy_model = kwargs.get("spacy_model", self._spacy_model_name)
        self._spacy_model_name = spacy_model

        random_state = kwargs.get("random_state", 42)
        test_size    = kwargs.get("test_size", 0.2) 

        X, y_raw, label_counts = self._extract_xy(entries, only_functional)
        if len(set(y_raw)) < 2:
            return {"error": "Need at least two distinct labels.", "label_counts": label_counts}
        
        print("DEVICE =", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "CPU")

        # Encode labels
        self.label_encoder = LabelEncoder()
        y = self.label_encoder.fit_transform(y_raw)
        self.label_order_ = list(self.label_encoder.classes_)
        num_labels = len(self.label_order_)


        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        collator = DataCollatorWithPadding(self.tokenizer)
        id2label = {i: lbl for i, lbl in enumerate(self.label_order_)}
        label2id = {v: k for k, v in id2label.items()}

        # Metrics for Trainer
        acc_metric = evaluate.load("accuracy")
        f1_metric  = evaluate.load("f1")
        try:
            roc_auc_metric = evaluate.load("roc_auc")
        except Exception:
            roc_auc_metric = None

        def compute_metrics(eval_pred):
            logits, labels = eval_pred
            preds = np.argmax(logits, axis=1)
            out = {}
            out.update(acc_metric.compute(predictions=preds, references=labels))
            out.update(f1_metric.compute(predictions=preds, references=labels, average="macro"))
            if roc_auc_metric is not None and num_labels == 2:
                probs = torch.softmax(torch.tensor(logits), dim=1).numpy()[:, 1]
                out.update(roc_auc_metric.compute(
                    prediction_scores=probs, references=labels, average="macro"
                ))
            return out
        
        ctx = {
            "X": X,
            "y": y,
            "y_raw": y_raw,
            "label_counts": label_counts,
            "num_labels": num_labels,
            "model_name": model_name,
            "max_length": max_length,
            "batch_size": batch_size,
            "num_epochs": num_epochs,
            "lr": lr,
            "weight_decay": weight_decay,
            "fp16": fp16,
            "logging_steps": logging_steps,
            "output_dir": output_dir,
            "freeze_encoder": freeze_encoder,
            "random_state": random_state,
            "test_size": test_size,
            "collator": collator,
            "id2label": id2label,
            "label2id": label2id,
            "compute_metrics": compute_metrics,
            "only_functional": only_functional
        }
        return ctx

    def _run_single_split(self, entries, **kwargs):

        ctx = self._prepare_common(entries, **kwargs)
        if "error" in ctx:
            return ctx
        
        X              = ctx["X"]
        y              = ctx["y"]
        label_counts   = ctx["label_counts"]
        num_labels     = ctx["num_labels"]
        model_name     = ctx["model_name"]
        max_length     = ctx["max_length"]
        batch_size     = ctx["batch_size"]
        num_epochs     = ctx["num_epochs"]
        lr             = ctx["lr"]
        weight_decay   = ctx["weight_decay"]
        fp16           = ctx["fp16"]
        logging_steps  = ctx["logging_steps"]
        output_dir     = ctx["output_dir"]
        freeze_encoder = ctx["freeze_encoder"]
        random_state   = ctx["random_state"]
        test_size      = ctx["test_size"]
        collator       = ctx["collator"]
        id2label       = ctx["id2label"]
        label2id       = ctx["label2id"]
        compute_metrics = ctx["compute_metrics"]

            
        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )


        # Tokenizer & datasets
        ds_train = _SimpleTextDataset(X_train, y_train, self.tokenizer, max_length)
        ds_test  = _SimpleTextDataset(X_test,  y_test,  self.tokenizer, max_length)

            # Model
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_name, num_labels=num_labels, id2label=id2label, label2id=label2id, problem_type="single_label_classification", use_safetensors=True
        )

        if freeze_encoder:
            for p in self.model.base_model.parameters():
                p.requires_grad = False

        # Training config
        args = TrainingArguments(
            output_dir=output_dir,
            per_device_train_batch_size=batch_size,
            per_device_eval_batch_size=batch_size,
            num_train_epochs=num_epochs,
            learning_rate=lr,
            weight_decay=weight_decay,
            eval_strategy="epoch",
            save_strategy="epoch",
            load_best_model_at_end=True,
            metric_for_best_model="f1",
            greater_is_better=True,
            logging_steps=logging_steps,
            fp16=bool(fp16),
            warmup_ratio=0.1,
            report_to=[],
            dataloader_num_workers=4,
            gradient_accumulation_steps=1
        )

        trainer = Trainer(
            model=self.model,
            args=args,
            train_dataset=ds_train,
            eval_dataset=ds_test,
            tokenizer=self.tokenizer,
            data_collator=collator,
            compute_metrics=compute_metrics,
            callbacks=[EarlyStoppingCallback(early_stopping_patience=2)]
        )

        trainer.train()

        eval_res = trainer.evaluate()

        with torch.no_grad():
            logits = trainer.predict(ds_test).predictions
            preds = logits.argmax(1)
            # --- Confusion matrices (raw and normalized) ---
            cm = confusion_matrix(y_test, preds, labels=list(range(num_labels)))
            cm_norm = confusion_matrix(y_test, preds, labels=list(range(num_labels)), normalize='true')


        report = classification_report(
            y_test, preds,
            labels=list(range(num_labels)),
            target_names=self.label_order_,
            output_dict=True, zero_division=0
        )

        acc = accuracy_score(y_test, preds)
        acc_bal = balanced_accuracy_score(y_test, preds)
        mcc = matthews_corrcoef(y_test, preds)

        roc_auc = None
        if num_labels == 2:
            probs = torch.softmax(torch.tensor(logits), dim=1).numpy()[:, 1]
            try:
                roc_auc = roc_auc_score(y_test, probs)
            except Exception:
                roc_auc = None

        return {
            "stats": {
                "result": {
                    "processed": len(X),
                    "level_used": self.level,
                    "accuracy": acc,
                    "balanced_accuracy": acc_bal,
                    "mcc": mcc,
                    "roc_auc": roc_auc,
                    "num_labels": num_labels,
                    "model_name": model_name,
                }
            },
            "data": {
                "classification_report": report,
                "label_counts": label_counts,
                "labels": self.label_order_,
                "trainer_eval": eval_res,
                "model": self.model,
                "tokenizer": self.tokenizer,
                "label_encoder": self.label_encoder,
            },
            "matrix": {
                "labels": self.label_order_,
                "confusion_matrix": cm.tolist(),
                "confusion_matrix_norm": cm_norm.tolist(),
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
        num_labels     = ctx["num_labels"]
        model_name     = ctx["model_name"]
        max_length     = ctx["max_length"]
        batch_size     = ctx["batch_size"]
        num_epochs     = ctx["num_epochs"]
        lr             = ctx["lr"]
        weight_decay   = ctx["weight_decay"]
        fp16           = ctx["fp16"]
        logging_steps  = ctx["logging_steps"]
        output_dir     = ctx["output_dir"]
        freeze_encoder = ctx["freeze_encoder"]
        random_state   = ctx["random_state"]
        collator       = ctx["collator"]
        id2label       = ctx["id2label"]
        label2id       = ctx["label2id"]
        compute_metrics = ctx["compute_metrics"]
        
        skf = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=random_state)

        all_y_test = []
        all_y_pred = []
        all_probs = [] if num_labels == 2 else None
        fold_metrics = []
        last_eval_res = None
        last_model = None

        for fold_idx, (train_idx, test_idx) in enumerate(skf.split(X, y), start = 1):
            X_train = [X[i] for i in train_idx]
            X_test = [X[i] for i in test_idx]
            y_train = y[train_idx]
            y_test = y[test_idx]

            ds_train = _SimpleTextDataset(X_train, y_train, self.tokenizer, max_length)
            ds_test  = _SimpleTextDataset(X_test,  y_test,  self.tokenizer, max_length)

            # New model per fold
            model = AutoModelForSequenceClassification.from_pretrained(
                model_name,
                num_labels=num_labels,
                id2label=id2label,
                label2id=label2id,
                problem_type="single_label_classification",
                use_safetensors=True,
            )

            if freeze_encoder:
                for p in model.base_model.parameters():
                    p.requires_grad = False

            fold_output_dir = os.path.join(output_dir, f"fold_{fold_idx}")

            args = TrainingArguments(
                output_dir=fold_output_dir,
                per_device_train_batch_size=batch_size,
                per_device_eval_batch_size=batch_size,
                num_train_epochs=num_epochs,
                learning_rate=lr,
                weight_decay=weight_decay,
                eval_strategy="epoch",
                save_strategy="epoch",
                load_best_model_at_end=True,
                metric_for_best_model="f1",
                greater_is_better=True,
                logging_steps=logging_steps,
                fp16=bool(fp16),
                warmup_ratio=0.1,
                report_to=[],
                dataloader_num_workers=4,
                gradient_accumulation_steps=1,
            )

            trainer = Trainer(
                model=model,
                args=args,
                train_dataset=ds_train,
                eval_dataset=ds_test,
                tokenizer=self.tokenizer,
                data_collator=collator,
                compute_metrics=compute_metrics,
                callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
            )

            trainer.train()

            last_eval_res = trainer.evaluate()
            last_model = model

            with torch.no_grad():
                logits = trainer.predict(ds_test).predictions
                preds = logits.argmax(1)

            all_y_test.append(y_test)
            all_y_pred.append(preds)

            if num_labels == 2:
                probs = torch.softmax(torch.tensor(logits), dim=1).numpy()[:, 1]
                all_probs.append(probs)

            acc = accuracy_score(y_test, preds)
            acc_bal = balanced_accuracy_score(y_test, preds)
            mcc = matthews_corrcoef(y_test, preds)

            fold_metrics.append({
                "fold": fold_idx,
                "accuracy": acc,
                "balanced_accuracy": acc_bal,
                "mcc": mcc,
                "n_test": int(len(y_test))
            })

            print(f"Fold {fold_idx}/{cv_folds}: {acc:.4f}, {acc_bal:.4f}, {mcc:.4f}")
        
        y_test_all = np.concatenate(all_y_test)
        y_pred_all = np.concatenate(all_y_pred)

        cm = confusion_matrix(y_test_all, y_pred_all, labels=list(range(num_labels)))
        cm_norm = confusion_matrix(y_test_all, y_pred_all, normalize="true", labels=list(range(num_labels)))

        report = classification_report(
            y_test_all, y_pred_all,
            labels=list(range(num_labels)),
            target_names=self.label_order_,
            output_dict=True,
            zero_division=0,
        )

        accs = [m["accuracy"] for m in fold_metrics]
        acc_bals = [m["balanced_accuracy"] for m in fold_metrics]
        mccs = [m["mcc"] for m in fold_metrics]

        roc_auc = None
        if num_labels == 2 and all_probs:
            probs_all = np.concatenate(all_probs)
            try:
                roc_auc = roc_auc_score(y_test_all, probs_all)
            except Exception:
                roc_auc = None

        self.model = last_model
        eval_res = last_eval_res

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
            }

        # One aggregate row
        stats["aggregate"] = {
            "processed": len(X),
            "level_used": self.level,
            "accuracy_mean": float(np.mean(accs)),
            "accuracy_std": float(np.std(accs)),
            "balanced_accuracy_mean": float(np.mean(acc_bals)),
            "balanced_accuracy_std": float(np.std(acc_bals)),
            "mcc_mean": float(np.mean(mccs)),
            "mcc_std": float(np.std(mccs)),
            "roc_auc": roc_auc,
            "num_labels": num_labels,
            "model_name": model_name,
            "cv_folds": cv_folds,
        }

        return {
            "stats": stats,
            "data": {
                "classification_report": report,
                "label_counts": label_counts,
                "labels": self.label_order_,
                "trainer_eval": eval_res,
                "model": self.model,
                "tokenizer": self.tokenizer,
                "label_encoder": self.label_encoder,
            },
            "matrix": {
                "labels": self.label_order_,
                "confusion_matrix": cm.tolist(),
                "confusion_matrix_norm": cm_norm.tolist(),
            }
        }

       


        

        
