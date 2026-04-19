"""Import Media module unittests."""
from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

from pyfakefs import fake_filesystem_unittest  # type: ignore

from file_manager.import_media import (ExifTimeError, exif_time2unix,
                                       file_type_from_name, get_import_list,
                                       read_file_time)

TEST_SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = TEST_SCRIPT_DIR.parent / "test_data"


class ImportMediaFakeFsTestCase(fake_filesystem_unittest.TestCase):
    """ Fake FS based tests. """
    def setUp(self):
        self.setUpPyfakefs()

    def test_file_type(self):
        self.assertEqual(file_type_from_name(Path('test.JPG')), 'jpg')
        self.assertEqual(file_type_from_name(Path('test.mpg')), 'mpg')
        self.assertEqual(file_type_from_name(Path('dir/test.mpg')), 'mpg')
        self.assertEqual(file_type_from_name(Path('dir/test.rx')), 'rx')
        self.assertEqual(file_type_from_name(Path('dir/test')), '')
        self.assertEqual(file_type_from_name(Path('/dir/path/')), '')


class ImportMediaTestCase(unittest.TestCase):
    """ Tests with real FS from test_data."""
    def test_exif_time2unix(self):
        with self.assertRaises(ExifTimeError):
            exif_time2unix('2018:06:30')
        with self.assertRaises(ExifTimeError):
            test_file_date = read_file_time(
                TEST_DATA_DIR / 'media/not_an_image', True)
        test_file_date = read_file_time(
            TEST_DATA_DIR / 'media/DSC06979.JPG', True)
        self.assertEqual(test_file_date, datetime(2018, 2, 19, 11, 5, 43))

    def test_import(self):
        missing_list = [
            '6TB-2 benchmark 2018-08-25 20-58-29.png',
            'IMG_0013.JPG',
        ]
        present_dict = {
            'DSC06979.JPG': TEST_DATA_DIR / 'storage/DSC06979.JPG',
            'IMG_0004.JPG': TEST_DATA_DIR / 'storage/tagged/IMG_0004.JPG',
        }
        present_tagged_dict = {
            'DSC06979.JPG': TEST_DATA_DIR / 'storage/tagged/DSC06979.JPG',
            'IMG_0004.JPG': TEST_DATA_DIR / 'storage/tagged/IMG_0004.JPG',
        }
        not_imported, already_imported = get_import_list(
            TEST_DATA_DIR / 'media', TEST_DATA_DIR / 'storage', [])
        self.assertEqual(sorted(missing_list), sorted(not_imported))
        self.assertEqual(present_dict, already_imported)
        not_imported, already_imported = get_import_list(
            TEST_DATA_DIR / 'media', TEST_DATA_DIR / 'storage/tagged', [])
        self.assertEqual(sorted(missing_list), sorted(not_imported))
        self.assertEqual(present_tagged_dict, already_imported)
