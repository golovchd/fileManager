#!/usr/bin/python2.7
"""Import Media module unittests."""


from datetime import datetime
import os
import unittest
from pyfakefs import fake_filesystem_unittest
import import_media


class ImportMediaFakeFsTestCase(fake_filesystem_unittest.TestCase):
  """ Fake FS based tests. """
  def setUp(self):
    self._test_data = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'test_data')
    self.setUpPyfakefs()

  def test_file_type(self):
    self.assertEqual(import_media.file_type_from_name('test.JPG'), 'jpg')
    self.assertEqual(import_media.file_type_from_name('test.mpg'), 'mpg')
    self.assertEqual(import_media.file_type_from_name('dir/test.mpg'), 'mpg')
    self.assertEqual(import_media.file_type_from_name('dir/test.rx'), 'rx')
    self.assertEqual(import_media.file_type_from_name('dir/test'), '')
    self.assertEqual(import_media.file_type_from_name('/dir/path/'), '')

class ImportMediaTestCase(unittest.TestCase):
  """ Tests with real FS from test_data."""
  def setUp(self):
    self._test_data = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'test_data')

  def test_exif_time2unix(self):
    with self.assertRaises(import_media.ExifTimeError):
      import_media.exif_time2unix('2018:06:30')
    with self.assertRaises(import_media.ExifTimeError):
      test_file_date = import_media.read_file_time(
          os.path.join(self._test_data, 'media/not_an_image'), True)
    test_file_date = import_media.read_file_time(
        os.path.join(self._test_data, 'media/DSC06979.JPG'), True)
    self.assertEqual(test_file_date, datetime(2018, 2, 19, 11, 05, 43))


if __name__ == '__main__':
  unittest.main()
