""" Module from

"""

from pandas import read_csv
import os
import re
import multiprocessing
import math

__numb__ = re.compile("([-]?\d+( BCE)?)")


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

    def find_embedding(self, *strings, window=50, ignore_center=False):
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

    def get(self, identifier):
        """ Get the Text object given its identifier

        :param identifier: Filename or identifier
        :type identifier: str
        :return: Text object
        :rtype: Text
        """
        return self.__files__[identifier]

    def find(self, *strings, multiprocess=None):
        """ Find files who contains given strings

        :param strings: Strings as multiple arguments
        :param multiprocess: Number of process to spawn
        :type multiprocess: int
        :return: Files who are matching the strings
        :rtype: generator
        """
        if isinstance(multiprocess, int):
            files = list(self.__files__.values())
            chunksize = int(math.ceil(len(files) / float(multiprocess)))
            kwargs = [
                (strings, files[chunksize * i:chunksize * (i + 1)])
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


def __find_multiprocess__(args):
    """ Find files who contains given strings

    :param args: Tuple where first element are Strings as list and second element is list of file objects
    :return: Files who are matching the strings
    :rtype: list
    """
    strings, files = args
    return [file for file in files if file.has_strings(*strings)]


def __window__(array, window, i):
    """ Compute embedding using i

    :param strings:
    :param window: Number of word to take left, then right [ len(result) = (2*window)+1 ]
    :param i: Index of the word
    :return: List of words
    """
    return array[max(i-window, 0):min(i+window+1, len(array))]