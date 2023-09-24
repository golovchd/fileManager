#!/usr/bin/python3
"""Import Media module unittests."""

import argparse
import sys
import unittest
from datetime import datetime
from pathlib import Path

from pyfakefs import fake_filesystem_unittest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))


import import_media  # noqa: E402


class ImportMediaFakeFsTestCase(fake_filesystem_unittest.TestCase):
    """ Fake FS based tests. """
    def setUp(self):
        self.setUpPyfakefs()

    def test_file_type(self):
        self.assertEqual(import_media.file_type_from_name('test.JPG'), 'jpg')
        self.assertEqual(import_media.file_type_from_name('test.mpg'), 'mpg')
        self.assertEqual(
            import_media.file_type_from_name('dir/test.mpg'), 'mpg')
        self.assertEqual(import_media.file_type_from_name('dir/test.rx'), 'rx')
        self.assertEqual(import_media.file_type_from_name('dir/test'), '')
        self.assertEqual(import_media.file_type_from_name('/dir/path/'), '')


class ImportMediaTestCase(unittest.TestCase):
    """ Tests with real FS from test_data."""
    def test_exif_time2unix(self):
        with self.assertRaises(import_media.ExifTimeError):
            import_media.exif_time2unix('2018:06:30')
        with self.assertRaises(import_media.ExifTimeError):
            test_file_date = import_media.read_file_time(
                TEST_DATA_DIR / 'media/not_an_image', True)
        test_file_date = import_media.read_file_time(
            TEST_DATA_DIR / 'media/DSC06979.JPG', True)
        self.assertEqual(test_file_date, datetime(2018, 2, 19, 11, 5, 43))

    def test_import(self):
        missing_list = [
            '6TB-2 benchmark 2018-08-25 20-58-29.png',
            'IMG_0013.JPG',
        ]
        present_dict = {
            'DSC06979.JPG': 'test_data/storage/DSC06979.JPG',
            'IMG_0004.JPG': 'test_data/storage/tagged/IMG_0004.JPG',
        }
        present_tagged_dict = {
            'DSC06979.JPG': 'test_data/storage/tagged/DSC06979.JPG',
            'IMG_0004.JPG': 'test_data/storage/tagged/IMG_0004.JPG',
        }
        not_imported, already_imported = import_media.get_import_list(
            'test_data/media', 'test_data/storage',
            filter_storage=False)
        self.assertEqual(sorted(missing_list), sorted(not_imported))
        self.assertEqual(present_dict, already_imported)
        not_imported, already_imported = import_media.get_import_list(
            'test_data/media', 'test_data/storage/tagged',
            filter_storage=False)
        self.assertEqual(sorted(missing_list), sorted(not_imported))
        self.assertEqual(present_tagged_dict, already_imported)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-v', '--verbose',
                            help='Print verbose output',
                            action='count', default=0)
    args = arg_parser.parse_args()
    import_media.logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=import_media.logging.WARNING - 10 * (
            args.verbose if args.verbose < 3 else 2))
    unittest.main()
