import os
import unittest
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import util


class TestChdirDecorator(unittest.TestCase):

    def setUp(self):
        self._tmp_dir = TemporaryDirectory()
        self.tmp_dir = Path(self._tmp_dir.name)

    def tearDown(self):
        self._tmp_dir.cleanup()

    def test_dir_changed(self):
        with util.chdir(self.tmp_dir):
            self.assertEqual(
                Path(os.getcwd()),
                self.tmp_dir
            )

    def test_original_returned(self):
        original = os.getcwd()
        with util.chdir(self.tmp_dir):
            pass
        self.assertEqual(os.getcwd(), original)


@dataclass
class SampleDataclass:
    num: int
    text: str


class TestDataclassFromDict(unittest.TestCase):

    def test_loop(self):
        d = dict(num=3, text="hey")
        self.assertEqual(
            asdict(
                util.dataclass_from_dict(
                    SampleDataclass,
                    d
                )
            ),
            d
        )
