import json
from pathlib import Path

class RoRoParser:
    """
    Parser class for processing .json data files of the RoRo dataset

    Attributes
    ----------
    path : str
        The path of the directory containing the .json files
    use_spacy : bool
        Should the parser load also SpaCy docs of the text
    """
    def __init__(self, options = None):

        """
        Initialize the parser with a dictionary of options

        Parameters
        ----------
        options : dict
            A dictionary of options
            Example:
                {
                    'path': 'data', # The path of the directory containing the .json files
                    'use_spacy': False # Should the parser load also SpaCy docs of the text
                    'spacy_model_name': 'ro_core_news_sm' # The name of the SpaCy model
                    'content_key': 'content' # Which JSON key contains the text
                    'title_key': 'title' # Which JSON key contains the title
                    'verbose': False # Should the parser print verbose output
                    'force': False # Should the parser overwrite already parsed data
                    'limit': None # The maximum number of files to parse
                }
        """
        self.path = 'data'
        self.use_spacy = False
        self.spacy_model_name = 'ro_core_news_sm'
        self.content_key = 'content'
        self.title_key = 'title'
        self.verbose = False
        self.force = False
        self.limit = None

        self.__data = []
        self.__errors = []
        self.__spacy = []
        self.__spacy_model = None

        if options:
            self.set(options)

        if self.verbose:
            print("[info] Initialized parser")
        pass

    def set(self, options):
        for key, value in options.items():
            setattr(self, key, value)

        return self
    
    def parse(self):
        if self.__data != [] and not self.force:
            print("[err] Parser already parsed data")
            return
        
        self.__data = []
        self.__meta = []
        self.__spacy = []

        for dirs, rel_path, fp in self.__iter_target_files():
            if self.limit and len(self.__data) >= self.limit:
                break

            text, meta = self.__safe_load_json(fp)

            meta.update({"rel_path": rel_path, "dirs": dirs})

            self.__data.append(text)
            self.__meta.append(meta)


        if self.verbose:
            print("[info] Parsed data")

        if self.__errors:
            print(f"[err] Errors: {self.__errors}")
        elif self.verbose:
            print("[info] No errors")

        if self.use_spacy:
            self.create_spacy_docs()

        if self.verbose:
            print("[info] Finished parsing")

        return self
    
    def head(self, limit = 2):
        """
        Print the first 'limit' number of parsed texts and their corresponding metadata

        :param limit: The number of texts to print. Defaults to 5
        :type limit: int
        """
        print(self.__data[:limit])
        print(self.__meta[:limit])

    def count_files(self):
        return len(self.__data)
        

    def __iter_target_files(self):
        """
        Iterate over the .json files in the root directory and its subdirectories
        
        Each item yielded is a tuple of (dirs, rel_path, fp) where
        
        - dirs is a list of strings representing the subdirectories in the path
        - rel_path is the relative path of the file from the root directory
        - fp is the Path object of the file
        """
        root = Path(self.path)

        for fp in root.rglob('*.json'):
            rel_path = fp.relative_to(root)
            dirs = list(rel_path.parts[:-1])
            yield dirs, rel_path.as_posix(), fp

    def __safe_load_json(self, fp):
        """
        Safely load a JSON file and extract the text, title and metadata from it
        
        Parameters
        ----------
        fp : Path
            The path of the JSON file
        
        Returns
        -------
        text : str
            The text of the JSON file
        meta : dict
            A dictionary containing the metadata of the JSON file
        """
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
            text = obj.get(self.content_key, "")
            title = obj.get(self.title_key, "")
            meta = obj.get("metadata", {})
            
            meta.update({"title": title})

            if isinstance(text, str):
                return text, meta
            else:
                raise Exception("JSON content or title is not a string")
        except Exception as e:
            if self.verbose:
                print(f"[skip] {fp}: {e}")
            self.__errors.append((fp, e))
            return "", {}   
        
    def __load_spacy(self):
        """
        Load a SpaCy model, and add the parser and sentencizer if they do not exist
        
        If the model could not be loaded, an error message is printed and the exception is stored in self.__errors
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None
        """
        
        if self.verbose:
            print("[info] Loading SpaCy")
        import spacy

        try:
            self.__spacy_model = spacy.load(self.spacy_model_name)
        except Exception as e:
            print(f"[err] Failed to load SpaCy model {self.spacy_model_name}: {e}")
            self.__errors.append((self.spacy_model_name, e))
            return
        
        if "parser" not in self.__spacy_model.pipe_names and "sentencizer" not in self.__spacy_model.pipe_names:
            if self.verbose:
                print("[info] Adding parser and sentencizer")
            self.__spacy_model.add_pipe("sentencizer")
        
        if self.verbose:
            print("[info] Loaded SpaCy")

    def create_spacy_docs(self):

        self.use_spacy = True

        if self.verbose:
            print("[info] Creating SpaCy docs")

        if self.__data == []:
            print("[err] Call parse() first")
            return
        
        if self.__spacy != [] and not self.force:
            print("[err] SpaCy docs already created")
            return
        
        if not self.__spacy_model:
            self.__load_spacy()

        for doc in self.__spacy_model.pipe(self.__data, batch_size=100, n_process=-1):
            self.__spacy.append(doc)

        
        if self.verbose:
            print("[info] Created SpaCy docs")


        

    
