# We import the main classes from the module
from archives_org_latin_toolkit import Repo, Metadata
from pprint import pprint

# We initiate a Metadata object and a Repo object
metadata = Metadata("./test/test_data/latin_metadata.csv")
# We want the text to be set in lowercase
repo = Repo("./test/test_data/archive_org_latin/", metadata=metadata, lowercase=True)

# We define a list of token we want to search for
tokens = ["ecclesiastico", "ecclesia", "ecclesiis"]

# We instantiate a result storage
results = []

# We iter over text having those tokens :
# Note that we need to "unzip" the list
for text_matching in repo.find(*tokens, multiprocess=4):

    # For each text, we iter over embeddings found in the text
    # We want 3 words left, 3 words right,
    # and we want to keep the original token (Default behaviour)
    for embedding in text_matching.find_embedding(*tokens, window=3, ignore_center=False):
        # We add it to the results
        results.append(embedding)

# We print the result (list of list of strings)
pprint(results)
