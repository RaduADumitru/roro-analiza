import re
import os
import csv 
import matplotlib.pyplot as plt
from collections import defaultdict
from pathlib import Path
import importlib

class RoRoAnalyzer:
    def __init__(self, parser):
        self.parser = parser

        self.cache = {}

    def run(self, analysis_name, query, uses_dict = False, **kwargs):

        """
        Run a named analysis on the parsed entries.

        :param analysis_name: name of the analysis module (e.g. "dataset_statistics")
        :param query: subfolder(s) to get (or None for all entries)
        :param uses_dict: return a dict of entries or a flat list
        :param **kwargs: additional keyword arguments to pass to the analysis module

        :return: result of the analysis (dict or scalar)
        """
        try:
            module = importlib.import_module(f"roro_module.analysis.{analysis_name}")
        except ImportError as e:
            raise ValueError(f"Analysis module {analysis_name} not found: {e}")
        
         # assume each module exports one class with same name but CamelCase
        class_name = "".join(part.capitalize() for part in analysis_name.split("_"))
        if not class_name.startswith("RoRo"):
            class_name = "RoRo" + class_name

        cls = getattr(module, class_name, None)
        if cls is None:
            raise ValueError(f"Class {class_name} not found in module {analysis_name}")
        
        analysis_instance = cls()

        # get entries
        if query is not None:
            entries = self.parser.get(query)
            if not uses_dict:
                entries = self.__flatten_dict(entries)
        elif uses_dict:
            entries = self.parser.get()
        else:
            entries = self.parser.get_flat()

        if entries is None:
            raise ValueError("No entries found")

        result = analysis_instance.run(entries, **kwargs)

        self.cache = {
            "name": analysis_name,
            "query": query,
            "result": result,
        }
        return result
    
    def __flatten_dict(self, d):
        items = []
        for k, v in d.items():
            if isinstance(v, dict):
                items.extend(self.__flatten_dict(v))
            else:
                items.append(v)
        return items
            
        
    def save_csv(self, out_csv="analysis"):
        """
        Saves the current statistics to a CSV file.

        If the cache is empty or does not contain the `stats` key, this function will print an error message and return.

        The CSV file will have the following structure:

        - The first column is the key (folder name or "(root)")
        - The following columns are the values for each key, in the order specified by the `all_fields` set.
        - If a key is missing a particular field, the value will be an empty string.

        The filename is specified by the `out_csv` parameter, which defaults to "analysis.csv".
        """
        if not self.cache:
            print("[err] Nothing in cache")
            return

        result = self.cache["result"]
        if "stats" not in result:
            print("[err] Cache does not contain folder stats")
            return
        
        stats = result["stats"]
        
        if not isinstance(stats, dict) or not all(isinstance(v, dict) for v in stats.values()):
            print("[err] Statistics is not a dict-of-dicts; cannot save clean CSV")
            return

        # collect all field names across all inner dicts
        all_fields = set()
        for v in stats.values():
            all_fields.update(v.keys())
        all_fields = sorted(all_fields)

        out_csv = f"stats/{self.cache['name']}/{out_csv}"

        # if the folders do not exist, create it
        os.makedirs(os.path.dirname(out_csv+".csv"), exist_ok=True)

        # if the filename already exists, append a number to it
        if os.path.exists(out_csv+'.csv'):
            i = 1
            while os.path.exists(f"{out_csv}-{i}.csv"):
                i += 1
            out_csv = f"{out_csv}-{i}"

        out_csv += ".csv"
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            header = ["key"] + all_fields
            w.writerow(header)

            for k, v in stats.items():
                row = [k] + [v.get(field, "") for field in all_fields]
                w.writerow(row)

        print(f"[Analyzer] Saved CSV -> {out_csv}")

        return 
    
    def plot(self, out_prefix="analysis", plots_dir="plots", **kwargs):
        if not self.cache:
            print("[err] Nothing in cache")
            return

        result = self.cache["result"]
        if "stats" not in result:
            print("[err] Cache does not contain folder stats")
            return
        
        stats = result["stats"]
        
        if not isinstance(stats, dict) or not all(isinstance(v, dict) for v in stats.values()):
            print("[err] Statistics is not a dict-of-dicts; cannot plot")
            return

        # collect all field names across all inner dicts
        all_fields = set()
        for v in stats.values():
            all_fields.update(v.keys())
        all_fields = sorted(all_fields)

        os.makedirs(plots_dir, exist_ok=True)

        for field in all_fields:
            labels = list(stats.keys())
            values = [v.get(field, 0) for v in stats.values()]

            plt.figure(figsize=(12, 6))
            plt.bar(labels, values)
            plt.xticks(rotation=45)
            plt.title(f"{field} per folder")
            plt.xlabel("Folder")
            plt.ylabel(field)
            plt.tight_layout()

            filename = f"{plots_dir}/{self.cache['name']}/{out_prefix}_{field}.png"

            # if the folders do not exist, create it
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            # if the filename already exists, append a number to it
            if os.path.exists(filename):
                i = 1
                while os.path.exists(f"{filename[:-4]}-{i}.png"):
                    i += 1
                filename = f"{filename[:-4]}-{i}.png"
            plt.savefig(filename)
            plt.close()

            print(f"[Analyzer] Saved plot -> {filename}")
