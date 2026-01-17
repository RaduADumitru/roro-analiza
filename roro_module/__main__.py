from .parser import RoRoParser
from .analyzer import RoRoAnalyzer
from .cleaner import RoRoCleaner
from .shuffler import RoRoShuffler
from .shuffler_db import RoRoShufflerDatabase
import json

def Cleaner():
    cleaner = RoRoCleaner('ignore/data-to-clean')
    count, folder_counts = cleaner.remove_empty()

    print(json.dumps(folder_counts, indent=4, ensure_ascii=False))
    print("\n\n")

    count, folder_counts = cleaner.remove_duplicate_gazetas()

    print(json.dumps(folder_counts, indent=4, ensure_ascii=False))
    print("\n\n")

    flags = cleaner.flag_duplicate_sentences()

    print(json.dumps(flags, indent=4, ensure_ascii=False))

def Statistics():
     # use_spacy false ca sa se incarce cu batching.
    # daca folosim PC cu multa memorie si placa video, se poate marca use_spacy True si analizatoarele vor rula mai repede
    parser = RoRoParser({'path': 'ignore/data-work', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    # Salvam un dataset statistic cu toate datele
    # Al doilea parametru este None deoarece nu facem un subquery
    #analyzer.run('dataset_statistics', None) 

    #analyzer.save_csv('dataset_statistics-2.csv')

    # Analiza pe Judete si Raioane
    # Vrem ca rezultatul sa includa regiunile, deci level = 1
    # Al treilea parametru este fals deoarece analizatorul nu trebuie sa primeasca un dict, ci un array flat
    # Level este un parametru **kwargs utilizat de sentence_stats pentru a decide la ce nivel sa faca analiza  
    analyzer.run('sentence_stats', None, False, level=0)

    analyzer.save_csv('dataset_2')

    analyzer.plot('dataset_2')

def Classifiers():
    
    parser = RoRoParser({'path': 'ignore/shuffler_db_romania_2200', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    result = analyzer.run('logistic_reg_tf_idf_classifier', None, False, level=0, verbose=True, only_functional = False, cv_folds=5)

    print(result)

    analyzer.save_csv('13_01_shuffled_regions_all_kfolds_5')

    analyzer.save_csv_matrix('13_01_shuffled_regions_all_kfolds_5')

def ClassifiersBERT():
    
    parser = RoRoParser({'path': 'ignore/data-work/Romania/Crisana', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    result = analyzer.run('bert_classifier', None, False, level=0, model_name="readerbench/RoBERT-small", fp16=False, max_length=128, freeze_encoder=True, cv_folds=3)

    print(result)

    analyzer.save_csv('bert_crisana_nocv')

    analyzer.save_csv_matrix('bert_crisana_nocv')

def ClassifiersLogRegBERT():
    
    parser = RoRoParser({'path': 'ignore/data-work/Romania/Crisana', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm', 'verbose': True})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    result = analyzer.run('bert_logistic_regression_classifier', None, False, level=0, verbose=True, cv_folds=3)

    print(result)

    analyzer.save_csv('crisana_test')

    analyzer.save_csv_matrix('crisana_test')

def StatsClassifiers():
    parser = RoRoParser({'path': 'ignore/data-work/Romania/Crisana', 'verbose': True, 'use_spacy': True, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    result = analyzer.run('sentence_stats_classifier', None, False, level=0, verbose=True)

    analyzer.save_csv('test_statsc_crisana')

    analyzer.save_csv_matrix('test_statsc_crisana')

def Shuffle():
    parser = RoRoParser({'path': 'ignore/data-work/Romania/Ardeal', 'verbose': True, 'use_spacy': False})

    parser.parse()

    shuffler = RoRoShuffler(parser)

    shuffler.setLevel(0)

    shuffler.setTargetWordCount(100)

    shuffler.setOutputPath("ignore/shuffler_db_test")

    shuffler.run()
if __name__ == "__main__":
    Classifiers()

