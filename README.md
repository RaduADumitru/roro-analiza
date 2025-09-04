Folder-ul Data contine datele din dataset-ul RoRo la data de 27.08.2025

Folder-ul data-cleaned contine datele curatate cu ajutorul fisierului SRC_01/src/cleanup-data-cleaned.py, care elimina textele cu content gol si gazetele din care textul s-a preluat gresit (daca o gazeta are > 50% texte identice)

In roro_module, se gaseste un exemplu de utilizare a modulului parser si analyzer (**main**).
In folder-ul analysis se pot crea analizatoare RoRoAnalyzerName care iau un set de date RoRoEntry (cu campul .doc SpaCy sau fara), si returneaza un dictionar cu un camp "stats", care este un dictionar de dictionare (cheia este categorie (Banat, Ardeal, Romania, etc.) si valoarea este un dictionar de campuri si valori ({"nr_cuvinte": 3, "procent" : 0.5}))

Analizatorul logistic_reg_tf_idf_classifier poate fi rulat cu only_functional = False pentru a folosi toate cuvintele din text, si only_functional = True (ruleaza mult mai incet) pentru a folosi doar cuvinte functionale, definite ca spacy {"ADP","CCONJ","SCONJ","PRON","DET","AUX","PART","INTJ"} pe post de dictionar.
