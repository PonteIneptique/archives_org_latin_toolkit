""" Module from http://github.com/ponteineptique/archives_org_latin_toolkit

"""

from pandas import read_csv
import os
import re
import multiprocessing
import math
from random import randrange
from collections import Counter
import csv

__numb__ = re.compile("([-]?\d+( BCE)?)")


def find_sub_list(subliste, liste):
    # While not found
    # get .index(subliste[0])
    # check if l
    sub_len = len(subliste)
    for i in range(0, liste.count(subliste[0])):
        start = liste.index(subliste[0])
        end = start + sub_len
        if subliste == liste[start:end]:
            return start, end


def bce(x):
    """ Format A BCE string

    :param x: Value to parse
    :type x: str
    :return: Parsed numeral
    :rtype: str

    """
    if "BCE" in x:
        return ("-" + x.replace(" BCE", "")).replace("--", "-")
    return x


def period(x):
    """ Parse a period in metadata. If there is multiple dates, returns the mean

    :param x: Value to parse
    :type x: str
    :return: Parsed numeral
    :rtype: int
    """
    dates = [
        int(bce(number))
        for number, _ in __numb__.findall(x)

    ]
    return math.ceil(sum(dates)/len(dates))


class Metadata:
    """ Metadata object for a file

    :param csv_file: Path to the CSV file to parse
    :type csv_file: str
    """

    def __init__(self, csv_file):
        self.__csv__ = read_csv(
            csv_file,
            delimiter="\t",
            index_col=0,
            dtype={
                "identifier": str,
                "creator": str,
                "title": str,
                "date of publication": str
            },
            converters={
                "date of composition": period
            },
            encoding="latin1"
        )

    def getDate(self, identifier):
        """ Get the date of a text given its identifier

        :param identifier: Filename or identifier
        :type identifier: str
        :return: Date of composition
        :rtype: int
        """
        return self.__csv__.get_value(identifier.split("/")[-1], "date of composition")


class Text:
    """ Text reading object for archive_org

    :param file: File path
    :type file: str
    :param metadata: Metadata registry
    :type metadata: Metadata
    :param lowercase: Clean Text will be in lowercase
    :type lowercase: bool

    :ivar name: Name of the file
    :type name: str
    :ivar composed: Date of composition
    :type composed: int

    """

    __entities = re.compile("&\w+;")
    __punct = re.compile("[^a-zA-Z]+")
    __space = re.compile("[\s]+")

    def __init__(self, file, metadata=None, lowercase=False):
        self.__file__ = file
        self.__date__ = None
        self.__raw__ = None
        self.__clean__ = None
        self.__lower__ = lowercase
        self.__metadata__ = metadata

    @property
    def name(self):
        return self.__file__.split("/")[-1]

    @property
    def composed(self):
        if self.__metadata__:
            if not self.__date__:
                self.__date__ = self.__metadata__.getDate(self.__file__)
            return self.__date__

    @property
    def raw(self):
        if not self.__raw__:
            with open(self.__file__) as f:
                self.__raw__ = f.read()
        return self.__raw__

    @property
    def clean(self):
        """ Clean version of the text : normalized space, remove new line, dehyphenize, remove punctuation and number.

        """
        if not self.__clean__:
            self.__clean__ = self.__space.sub(
                " ",
                self.__punct.sub(
                    " ",
                    self.__entities.sub(" ", self.raw.replace("-\n", "").replace("\n", " "))
                )
            )
            if self.__lower__:
                self.__clean__ = self.__clean__.lower()
        return self.__clean__

    def cleanUp(self):
        """ Clean textual information and free RAM
        """
        self.__raw__ = None
        self.__clean__ = None

    def has_strings(self, *strings):
        """ Check if given string is in the file

        :param strings: Strings as multiple arguments
        :return: If found, return True
        :rtype: bool
        """
        status = False
        for string in strings:
            if string in self.raw:
                status = True
                break
        return status

    def find_embedding(self, *strings, window=50, ignore_center=False, memory_efficient=True):
        """ Check if given string is in the file

        :param strings: Strings as multiple arguments
        :param window: Number of lines to retrieve
        :param ignore_center: Remove the word found from the embedding
        """

        array = self.clean.split()
        strings = list(strings)
        for i, x in enumerate(array):
            if x in strings:
                if ignore_center:
                    yield [w for w in __window__(array, window, i) if w != x]
                else:
                    yield __window__(array, window, i)

        if memory_efficient:
            self.cleanUp()

    def random_embedding(self, grab, window=50, avoid=None, memory_efficient=True, _taken=None, _generator=True):
        """ Search for random sentences in the text. Can avoid certain words

        :param grab: Number of random sequence to retrieve
        :type grab: int
        :param window: Number of lines to retrieve
        :type window: int
        :param avoid: List of lemmas NOT TO be included in random
        :param _taken: Used internally to check we do not sample with the same element again
        :param _generator: If set to True, returns the window and its index in the text
        :return: Generator with random texts

        .. note:: Right now, new window found are not added to _taken, which is problematic
        """
        split_text = self.clean.split()
        max_range = len(split_text)
        if not _taken:
            _taken = []
        if not avoid:
            avoid = []

        # For each random sample we need to get
        for i in range(0, grab):
            # We get a random index (starting at window)
            ri = randrange(window, max_range, step=(window*2)+1+randrange(0, 5))
            # We check that the new index does not belong to any previous range
            if True in [ri in range(*t) for t in _taken]:
                w, _t = next(
                    self.random_embedding(1, window, avoid, memory_efficient, _taken=_taken, _generator=False)
                )
                _taken.append(_t)
            else:
                w = __window__(split_text, window, ri)
                # We check avoided lemma is not in the window
                if True in [word in avoid for word in w] or len(w) < window+1:
                    w, _t = next(
                        self.random_embedding(1, window, avoid, memory_efficient, _taken=_taken, _generator=False)
                    )
                    _taken.append(_t)
                else:
                    _taken.append((ri-window*2, ri+window*2))
            if _generator:
                yield w
            else:
                yield w, _taken[-1]
        if memory_efficient:
            self.cleanUp()


class Repo:
    """ Repo reading object for archive_org

    :param file: File path
    :type file: str
    :param metadata: Metadata registry
    :type metadata: Metadata
    :param lowercase: Clean Text will be in lowercase
    :type lowercase: bool
    """
    def __init__(self, directory, metadata=None, lowercase=False):
        self.__directory__ = directory
        self.__metadata__ = metadata

        self.__files__ = {
            file: Text(os.path.join(root, file), metadata, lowercase=lowercase)
            for root, dirs, files in os.walk(directory)
            for file in files
        }

    @property
    def metadata(self):
        return self.__metadata__

    def get(self, identifier):
        """ Get the Text object given its identifier

        :param identifier: Filename or identifier
        :type identifier: str
        :return: Text object
        :rtype: Text
        """
        return self.__files__[identifier]

    def find(self, *strings, multiprocess=None, memory_efficient=True):
        """ Find files who contains given strings

        :param strings: Strings as multiple arguments
        :param multiprocess: Number of process to spawn
        :type multiprocess: int
        :param memory_efficient: Drop the content of files to avoid filling the ram with unused content
        :type memory_efficient: bool
        :return: Files who are matching the strings
        :rtype: generator
        """
        if isinstance(multiprocess, int):
            files = list(self.__files__.values())
            chunksize = int(math.ceil(len(files) / float(multiprocess)))
            kwargs = [
                (strings, files[chunksize * i:chunksize * (i + 1)], memory_efficient)
                for i in range(multiprocess)
            ]
            pool = multiprocessing.Pool(multiprocess)
            for result in pool.imap_unordered(__find_multiprocess__, kwargs):
                for element in result:
                    yield element
        else:
            for file in self.__files__.values():
                if file.has_strings(*strings):
                    yield file
                self.__files__[file.name].__raw__ = None
                self.__files__[file.name].__clean__ = None


class Search:
    """ Tool to make search, caching and corpus building easier for further requests

    :param filename: Name of the file to which you want to save results (Without extension !)
    :param lemmas: Strings as multiple arguments
    :param multiprocess: Number of process to spawn
    :type multiprocess: int
    :param memory_efficient: Drop the content of files to avoid filling the ram with unused content
    :type memory_efficient: bool
    """
    def __init__(
            self,
            repository, filename, *lemmas,
            ignore_center=True, window=50,
            multiprocess=None, memory_efficient=True
        ):
        self.__repository__ = repository
        self.__filename__ = filename
        self.__window__ = window
        self.__ignore_center = ignore_center
        self.__lemmas__ = list(lemmas)
        self.__multiprocess__ = multiprocess
        self.__memory_efficient__ = memory_efficient
        self.__results_dispatch__ = Counter()

    @property
    def filename(self):
        return self.__filename__+".csv"

    @property
    def random_filename(self):
        return self.__filename__+".rdm.csv"

    @property
    def repository(self):
        """

        :return:
        :rtype: Repository
        """
        return self.__repository__

    def execute(self):
        """ Execute the research on the corpus

        :return: A generator of tuples (date, text id, window)
        """
        # We iter over text having those tokens :
        # Note that we need to "unzip" the list
        for text_matching in self.repository.find(
                *self.__lemmas__,
                multiprocess=self.__multiprocess__,
                memory_efficient=self.__memory_efficient__
        ):
            # For each text, we iter over embeddings found in the text
            # We want WINDOW words left, WINDOW words right,
            # and we want to keep the original token (Default behaviour)
            date = text_matching.composed
            for embedding in text_matching.find_embedding(
                    *self.__lemmas__,
                    window=self.__window__,
                    ignore_center=self.__ignore_center,
                    memory_efficient=self.__memory_efficient__
            ):
                # We add it to the results
                yield (date, text_matching.name, " ".join(embedding))

            if self.__memory_efficient__:
                # This prevent memory struggle
                self.repository.__files__[text_matching.name].__raw__ = None
                self.repository.__files__[text_matching.name].__clean__ = None
                del text_matching

    def to_csv(self, _function="execute", with_random=True):
        if _function == "execute":
            _function = self.execute
            _counter = True
            filename = self.filename
        else:
            _function = self.random
            _counter = False
            with_random = False
            filename = self.random_filename

        with open(filename, "w", newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerow(["date", "source", "tokens"])

        stack = []
        for row in _function():
            if _counter:
                self.__results_dispatch__[row[1]] += 1
            stack.append(list(row))
            if len(stack) == 50:
                with open(filename, "a", newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter='\t')
                    writer.writerows(stack)
                stack = []

        with open(filename, "a", newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerows(stack)

        if with_random:
            self.to_csv("random", with_random=False)

        return True

    def from_csv(self, with_random=True):
        read = lambda file: read_csv(
            csvfile,
            delimiter="\t",
            index_col=None,
            dtype={
                "date": int,
                "source": str,
                "tokens": str
            },
            encoding="utf8"
        )
        with open(self.filename, "r") as csvfile:
            corpus = read(csvfile)
        if with_random and os.path.isfile(self.random_filename):
            with open(self.random_filename, "r") as csvfile:
                random_corpus = read(csvfile)
            return corpus, random_corpus
        else:
            return corpus

    def random(self):
        for text, grab_number in self.__results_dispatch__.items():
            for match in self.repository.get(text).random_embedding(
                grab_number, window=self.__window__, avoid=self.__lemmas__,
                memory_efficient=self.__memory_efficient__
            ):
                yield self.repository.get(text).composed, text, match

            if self.__memory_efficient__:
                # This prevent memory struggle
                self.repository.get(text).__raw__ = None
                self.repository.get(text).__clean__ = None


def __find_multiprocess__(args):
    """ Find files who contains given strings

    :param args: Tuple where first element are Strings as list and second element is list of file objects
    :return: Files who are matching the strings
    :rtype: list
    """
    strings, files, memoryefficient = args
    results = []
    while len(files):
        file = files.pop()
        if file.has_strings(*strings):
            results.append(file)
            file.cleanUp()
    return results


def __window__(array, window, i):
    """ Compute embedding using i

    :param strings:
    :param window: Number of word to take left, then right [ len(result) = (2*window)+1 ]
    :param i: Index of the word
    :param memory_efficient: Drop the content of files to avoid filling the ram with unused content
    :type memory_efficient: bool
    :return: List of words
    """
    return array[max(i-window, 0):min(i+window+1, len(array))]
