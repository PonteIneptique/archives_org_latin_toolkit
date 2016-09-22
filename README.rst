.. image:: https://coveralls.io/repos/github/PonteIneptique/archives_org_latin_toolkit/badge.svg?branch=master
    :target: https://coveralls.io/github/PonteIneptique/archives_org_latin_toolkit?branch=master
.. image:: https://travis-ci.org/PonteIneptique/archives_org_latin_toolkit.svg?branch=master
    :target: https://travis-ci.org/PonteIneptique/archives_org_latin_toolkit
.. image:: https://badge.fury.io/py/archives_org_latin_toolkit.svg
    :target: https://badge.fury.io/py/archives_org_latin_toolkit
.. image:: https://readthedocs.org/projects/archives-org-latin-toolkit/badge/?version=latest
    :alt: Documentation
    :target: https://archives-org-latin-toolkit.readthedocs.io

What ?
######

This piece of software is intended to be used with the 11K Latin Texts produced by David Bamman ( http://www.cs.cmu.edu/~dbamman/latin.html ). \
It supports only the plain text formats and the metadata github repo CSV file. This has been tested with *Python3* only. \
I welcome any new functions or backward compatibility support.

How to install ?
################

- **With development version:**
    - Clone the repository : :code:`git clone https://github.com/ponteineptique/archives_org_latin_toolkit.git`
    - Go to the directory : :code:`cd archives_org_latin_toolkit`
    - Install the source with develop option : :code:`python setup.py install`
- **With pip:**
    - Install from pip : :code:`pip install archives_org_latin_toolkit`

Example
#######

The following example should run with the data in tests/test_data. The example can be run with :code:`python example.py`

.. code-block:: python

    # We import the main classes from the module
    from archives_org_latin_toolkit import Repo, Metadata
    from pprint import pprint

    # We initiate a Metadata object and a Repo object
    metadata = Metadata("./test/test_data/latin_metadata.csv")
    # We want the text to be set in lowercase
    repo = Repo("./test/test_data/archive_org_latin/", metadata=metadata, lowercase=True)

    # We define a list of token we want to search for
    tokens = ["ecclesiastico", "ecclesia", "ecclesiis", "&quot;"]

    # We instantiate a result storage
    results = []

    # We iter over text having those tokens :
    # Note that we need to "unzip" the list
    for text_matching in repo.find(*tokens):

        # For each text, we iter over embeddings found in the text
        # We want 3 words left, 3 words right,
        # and we want to keep the original token (Default behaviour)
        for embedding in text_matching.find_embedding(*tokens, window=3, ignore_center=False):
            # We add it to the results
            results.append(embedding)

    # We print the result (list of list of strings)
    pprint(results)

