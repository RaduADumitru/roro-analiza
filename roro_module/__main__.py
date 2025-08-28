from .parser import RoRoParser

if __name__ == "__main__":
    parser = RoRoParser({'limit': 10, 'path': 'data-cleaned', 'verbose': True, 'use_spacy': True, 'spacy_model_name': 'ro_core_news_sm'})

    parser.parse()

    parser.head()

    print(f"Parsed {parser.count_files()} files")