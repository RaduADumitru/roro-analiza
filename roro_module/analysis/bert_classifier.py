from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import os, psutil
import numpy as np

from sklearn.model_selection import train_test_split
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
    batch_size = 8
    num_epochs = 8
    lr: float = 1e-3
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

    def run(self, entries, **kwargs):

        # Allow overrides via analyzer.run kwargs
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
        freeze_encoder  = kwargs.get("freeze_encoder", False)  # optional speedup on CPU

        random_state = kwargs.get("random_state", 42)
        test_size = kwargs.get("test_size", 0.2)

        X, y_raw, label_counts = self._extract_xy(entries)
        if len(set(y_raw)) < 2:
            return {"error": "Need at least two distinct labels.", "label_counts": label_counts}

        # Encode labels
        self.label_encoder = LabelEncoder()
        y = self.label_encoder.fit_transform(y_raw)
        self.label_order_ = list(self.label_encoder.classes_)
        num_labels = len(self.label_order_)

        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )

        
        # Tokenizer & datasets
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        ds_train = _SimpleTextDataset(X_train, y_train, self.tokenizer, max_length)
        ds_test  = _SimpleTextDataset(X_test,  y_test,  self.tokenizer, max_length)
        collator = DataCollatorWithPadding(self.tokenizer)
        
        id2label = {i: lbl for i, lbl in enumerate(self.label_order_)}
        label2id = {v: k for k, v in id2label.items()}

        # Model
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_name, num_labels=num_labels, id2label=id2label, label2id=label2id, problem_type="single_label_classification",
        )

        if freeze_encoder:
            for p in self.model.base_model.parameters():
                p.requires_grad = False

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

        # Memory snapshot
        proc = psutil.Process(os.getpid())
        mem_before = proc.memory_info().rss / 1024**2

        trainer.train()

        mem_after = proc.memory_info().rss / 1024**2
        eval_res = trainer.evaluate()

        # sklearn-style report for parity with TF-IDF flow
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

        # keep structure similar to your TF-IDF version
        return {
            "stats": {
                "result": {
                    "processed": len(X),
                    "level_used": self.level,
                    "accuracy": acc,
                    "balanced_accuracy": acc_bal,
                    "mcc": mcc,
                    "roc_auc": roc_auc,
                    "memory_mb_before": round(mem_before, 1),
                    "memory_mb_after": round(mem_after, 1),
                    "num_labels": num_labels,
                    "model_name": model_name,
                    # BERT has no linear n-gram weights → no top_features here
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
