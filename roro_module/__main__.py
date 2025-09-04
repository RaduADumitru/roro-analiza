from .parser import RoRoParser
from .analyzer import RoRoAnalyzer
from .cleaner import RoRoCleaner
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
    parser = RoRoParser({'path': 'ignore/data-work/romania/Ardeal', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm'})

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

    analyzer.save_csv('ardeal')

    analyzer.plot('ardeal')

def Classifiers():
    
    parser = RoRoParser({'path': 'ignore/data-work/romania', 'verbose': True, 'use_spacy': False, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    analyzer = RoRoAnalyzer(parser)

    result = analyzer.run('logistic_reg_tf_idf_classifier', None, False, level=0, only_functional=True, verbose=True)

    print(result)

    analyzer.save_csv('regiuni-ro-only-functional')

if __name__ == "__main__":
    Classifiers()

