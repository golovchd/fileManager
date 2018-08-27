#!/usr/bin/python2.7
# Import photos from media to storage


import argparse
from datetime import datetime, timedelta
import exifread
import os
import string


_COMPARE_TIME_DIFF = timedelta(2)

class ExifTimeError(Exception):
  pass


class IterateDir(Exception):
  pass


def file_type_from_name(file_name):
  return os.path.splitext(file_name)[1][1:].lower()


def exif_time2unix(exif_time):
  exif_time_str = str(exif_time)
  if len(exif_time_str) != 19:
    raise ExifTimeError('unexpectef ExifTime {}'.format(exif_time_str))
  exif_time_str = exif_time_str.replace(' ', ':', 1)
  exif_time_parts = exif_time_str.split(':')
  if len(exif_time_parts) != 6:
    raise ExifTimeError('unexpectef ExifTime {}'.format(exif_time_str))
  return datetime(int(exif_time_parts[0]),
                  int(exif_time_parts[1]),
                  int(exif_time_parts[2]),
                  int(exif_time_parts[3]),
                  int(exif_time_parts[4]),
                  int(exif_time_parts[5]))


def float2timestamp(float_timestamp):
  return datetime.fromtimestamp(float_timestamp)


def timestamp2exif_str(timestamp_obj):
  return timestamp_obj.strftime('%Y:%m:%d %H:%M:%S')


def read_file_time(file_name, exif_only=False):
  file_time = None
  if file_type_from_name(file_name) in MediaFiles.default_types['photo']:
    with open(file_name, 'rb') as file_obj:
      tags = exifread.process_file(file_obj, details=False)
      file_obj.close()
    file_time = tags.get('Image DateTime', None)
  if not file_time:
    if exif_only:
      raise ExifTimeError('unable to read ExifTime from {}'.format(file_name))
    file_timestamp = float2timestamp(os.stat(file_name).st_mtime)
    print 'mtime as file_time {}'.format(timestamp2exif_str(file_timestamp)),
  else:
    file_timestamp = exif_time2unix(file_time)
    print 'exif as file_time {}'.format(file_time),
  return file_timestamp

def compare_files(file_name_1, dir_path_1, dir_path_2, file_name_2=None):
  file_type = file_type_from_name(file_name_1)
  if not file_name_2:
    file_name_2 = file_name_1
  elif file_type != file_type_from_name(file_name_2):
    return False
  file_path_1 = os.path.join(dir_path_1, file_name_1)
  file_path_2 = os.path.join(dir_path_2, file_name_2)
  stat_1 = os.stat(file_path_1)
  stat_2 = os.stat(file_path_2)
  if file_type not in MediaFiles.default_types['photo']:
    print 'not a photo, match by size',
    return stat_1.st_size == stat_2.st_size

  if stat_1.st_size != stat_2.st_size:
    print 'Size diff,',
  else:
    print 'Size match,',
  if stat_1.st_mtime == stat_2.st_mtime:
    print 'mtime match in locations',
    return True

  exif_time_1 = read_file_time(file_path_1, True)
  mtime_2 = float2timestamp(stat_2.st_mtime)
  if exif_time_1 - mtime_2 > _COMPARE_TIME_DIFF:
    print 'second file too old {}'.format(timestamp2exif_str(mtime_2)),
    return False
  return exif_time_1 == read_file_time(file_path_2)


class MediaFilesIterator:
  def __init__(self, media):
    self.media = media
    self.dir_names = self.media.AllDirs()
    self.dir_idx = 0
    self.dir_count = len(self.dir_names)
    self.file_idx = 0
    self.init_dir()

  def __iter__(self):
    return self

  def init_dir(self, stop_iteration=False):
    if self.dir_idx < self.dir_count:
      self.cur_dir = self.media.DirFiles(self.dir_names[self.dir_idx])
      self.cur_dir_count = len(self.cur_dir)
      self.file_idx = 0
    else:
      self.cur_dir = None
      self.cur_dir_count = 0
      if stop_iteration:
        raise StopIteration()

  def next_file(self, stop_iteration=False):
    if self.file_idx < self.cur_dir_count:
      file_name = self.cur_dir[self.file_idx]
      self.file_idx += 1
      return (self.dir_names[self.dir_idx], file_name)
    if stop_iteration:
      raise StopIteration()
    else:
      raise IterateDir()

  def next(self):
    try:
      return self.next_file()
    except IterateDir:
      self.dir_idx += 1
      self.init_dir(True)
      return self.next_file()

class MediaFiles:
  default_types = {'photo':['jpg', 'arw'],
                   'video':['mts', 'mp4', 'mov']
                  }
  storage_dirs = ['Photos', 'Videos']

  def __init__(self, root, types=None):
    self.root = root
    self.count = 0
    if types:
      self.types = types
    else:
      self.types = MediaFiles.default_types
    self.SetAllTypes()
    self.import_list = {}

  def __iter__(self):
    return MediaFilesIterator(self)

  def SetAllTypes(self):
    self.all_types = []
    for _, types in self.types.iteritems():
      self.all_types.extend(types)

  def FileToImport(self, filename):
    file_type = file_type_from_name(filename)
    return file_type in self.all_types

  def AllDirs(self):
    return self.import_list.keys()

  def DirFiles(self, dir_name):
    if dir_name in self.import_list:
      return self.import_list[dir_name]

  def ReadDir(self, dir_path, files):
    dir_list = []
    for file_name in files:
      if self.FileToImport(file_name):
        dir_list.append(file_name)
    dir_count = len(dir_list)
    if dir_count:
#      print 'loading {} files from {}'.format(dir_count, dir_path)
      self.import_list[dir_path] = dir_list
      self.count += dir_count

  def ReadMedia(self, filter_storage=False):
    if filter_storage:
      dir_list = MediaFiles.storage_dirs
    else:
      dir_list = ['']
    for dir_name in dir_list:
      for root, dirs, files in os.walk(os.path.join(self.root, dir_name)):
        self.ReadDir(root, files)
    return self.count

  def FindFile(self, file_name, file_path=None):
    for dir_path, files in self.import_list.iteritems():
      # Skip Matching dirs to themselfs, will allow import from subdirs
      if dir_path == file_path:
        continue
      if file_name in files:
        if file_path and compare_files(file_name, file_path, dir_path):
          return dir_path
    return None


def ImportMedia(media_root, storage_root):
  media = MediaFiles(media_root)
  media.ReadMedia()
  print '{} contain {} files'.format(media_root, media.count)
  if not media.count:
    return 0
  storage = MediaFiles(storage_root)
  storage.ReadMedia(True)
  print '{} contain {} files'.format(storage_root, storage.count)
  count = 0
  present_count = 0
  for file_path, file_name in media:
    count += 1
    print 'Processing {} {}/{}'.format(file_name, count, media.count),
    storage_dir = storage.FindFile(file_name, file_path)
    if storage_dir:
      present_count += 1
      print 'present in {}'.format(storage_dir)
    else:
      print 'NOT present in storage'
  print '{} contain {} files, {} not present in {}'.format(
      media_root, media.count, count - present_count, storage_root)


def main():
  args = argparse.ArgumentParser(
      description='Import photos from media to storage')
  args.add_argument('--media', help='Media to import files from')
  args.add_argument('--storage', help='Storage to save imported media files')
  args.parse_args(namespace=args)
  ImportMedia(args.media, args.storage)

if __name__ == '__main__':
  main()


