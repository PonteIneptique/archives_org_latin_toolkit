from archives_org_latin_toolkit import *
from unittest import TestCase


class Test_Metadata(TestCase):
    def setUp(self):
        self.metadata = Metadata("./test/test_data/latin_metadata.csv")
        self.file1 = Text("./test/test_data/archive_org_latin/1887proacluentioo00cice", metadata=self.metadata)
        self.file2 = Text("./test/test_data/archive_org_latin/4621489", metadata=self.metadata)
        self.repo = Repo("./test/test_data/archive_org_latin/", metadata=self.metadata)
        self.maxDiff = 200

    def test_date(self):
        """ Ensure finding date works """
        self.assertEqual(
            self.metadata.getDate("1887proacluentioo00cice"), -74,
            "We should get the right date of composition"
        )
        self.assertEqual(
            self.metadata.getDate("path/to/4621489"), 1867,
            "We should get the right date of composition for filename"
        )

    def test_string_in(self):
        """ Ensure string checking works """
        self.assertEqual(
            self.file1.has_strings("hereditas"), True,
            "We should find hereditas in procaluentio"
        )
        self.assertEqual(
            self.file2.has_strings("hereditas"), False,
            "We should not find hereditas in 4621489"
        )
        self.assertEqual(
            self.file2.has_strings("hereditas", "descripta"), True,
            "We should not find hereditas in 4621489 but we should descripta"
        )
        self.assertEqual(
            self.file2.has_strings("hereditas", "hereditatumissia"), False,
            "We should find neither hereditas or my new word in 4621489"
        )

    def test_find_embedding(self):
        self.assertEqual(
            list(self.file2.find_embedding("hereditas", "descripta", window=5)),
            [["vita", "mihi", "narranda", "videtur", "qua", "descripta", "ad", "illam", "ipsam", "naturali", "quadam"]],
            "We should find neither hereditas or my new word in 4621489"
        )
        self.assertEqual(
            list(self.file2.find_embedding("hereditas", "descripta", window=5, ignore_center=True)),
            [["vita", "mihi", "narranda", "videtur", "qua", "ad", "illam", "ipsam", "naturali", "quadam"]],
            "We should find neither hereditas or my new word in 4621489"
        )
        self.assertEqual(
            list(self.file2.find_embedding("descriptae", "descripta", window=5, ignore_center=True)),
            [
                ["vita", "mihi", "narranda", "videtur", "qua", "ad", "illam", "ipsam", "naturali", "quadam"],
                ["compositam", "esse", "dicit", "ut", "res", "re", "vera", "vanae", "et", "dignae"]
             ],
            "We should find neither hereditas or my new word in 4621489"
        )

    def test_get_filename(self):
        """ Ensure text object have access to the date"""
        self.assertEqual(
            self.file1.name, "1887proacluentioo00cice",
            "We should get the right filename"
        )

    def test_get_date(self):
        """ Ensure text object have access to the date"""
        self.assertEqual(
            self.file1.composed, -74,
            "We should get the right date of composition"
        )

    def test_find_string_in_repo(self):
        """ Ensure we find object with string in it"""
        self.assertEqual(
            [file.composed for file in self.repo.find("hereditas")], [-74],
            "We should get the date of proacluentio"
        )
        self.assertCountEqual(
            [file.composed for file in self.repo.find("descripta", "doctorum")], [1758, 1867],
            "We should get the date of proacluentio"
        )

    def test_find_string_in_repo_multiprocess(self):
        """ Ensure we find object with string in it with multiprocess"""
        self.assertEqual(
            [file.composed for file in self.repo.find("hereditas", multiprocess=8)], [-74],
            "We should get the date of proacluentio"
        )
        self.assertCountEqual(
            [file.composed for file in self.repo.find("descripta", "doctorum", multiprocess=8)], [1758, 1867],
            "We should get the date of proacluentio"
        )

    def test_cleaning(self):
        """ Check that cleaning works well """
        self.assertEqual(
            self.file1.clean[:100],
            "tJrm Rivington s Educational List A First German Book By H S Beresford Webb is d German Examination ",
            "There should be no punctuation, double space, or number."
        )

    def test_cleanup(self):
        """ Check that cleaning works well """
        self.assertEqual(
            self.file1.clean[:100],
            "tJrm Rivington s Educational List A First German Book By H S Beresford Webb is d German Examination ",
            "We should access the string"
        )
        self.file1.cleanUp()
        self.assertEqual(
            (self.file1.__raw__, self.file1.__clean__), (None, None),
            "Cache should be clean"
        )
        self.assertEqual(
            self.file1.clean[:100],
            "tJrm Rivington s Educational List A First German Book By H S Beresford Webb is d German Examination ",
            "Cache should be rebuilt"
        )


    def test_cleaning_lowercase(self):
        """ Check that lowercase cleaning works well, also get text"""
        metadata = Metadata("./test/test_data/latin_metadata.csv")
        repo = Repo("./test/test_data/archive_org_latin/", metadata=metadata, lowercase=True)
        self.assertEqual(
            repo.get("1885denovahieros00sweduoft").clean[:100],
            "de nova hierosolyma et ejus doctrina caelesti ex auditis e caelo quibus praemittitur aliquid de novo",
            "There should be no punctuation, double space, number, entities or uppercase."
        )


