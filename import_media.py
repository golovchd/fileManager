#!/usr/bin/python3
"""Import photos from media to storage."""


import argparse
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import exifread

from utils import float2timestamp, timeobj2exif_str

_COMPARE_TIME_DIFF = timedelta(2)  # 2 days


class ExifTimeError(Exception):
    """Unable to read Exif Time from media file error."""


class IterateDir(Exception):
    """End of directory loop exception."""


def file_type_from_name(file_name):
    """Returns file's extension."""
    return os.path.splitext(file_name)[1][1:].lower()


def exif_time2unix(exif_time):
    """Converts exif_time to datetime.datetime object."""
    exif_time_str = str(exif_time)
    if len(exif_time_str) != 19:
        raise ExifTimeError(f'unexpectef ExifTime {exif_time_str}')
    exif_time_str = exif_time_str.replace(' ', ':', 1)
    exif_time_parts = exif_time_str.split(':')
    if len(exif_time_parts) != 6:
        raise ExifTimeError(f'unexpectef ExifTime {exif_time_str}')
    return datetime(int(exif_time_parts[0]),
                    int(exif_time_parts[1]),
                    int(exif_time_parts[2]),
                    int(exif_time_parts[3]),
                    int(exif_time_parts[4]),
                    int(exif_time_parts[5]))


def read_file_time(file_name, exif_only=False):
    """Returns file's time from exif or from mtime."""
    file_time = None
    if file_type_from_name(file_name) in MediaFiles.default_types['photo']:
        with open(file_name, 'rb') as file_obj:
            tags = exifread.process_file(file_obj, details=False)
            logging.debug(f"{file_name} tags {tags}")

        file_time = tags.get('EXIF DateTimeOriginal',
                             tags.get('Image DateTime', None))
    if not file_time:
        if exif_only:
            raise ExifTimeError(f"unable to read ExifTime from {file_name}")
        file_timestamp = float2timestamp(os.stat(file_name).st_mtime)
        logging.debug(
            f"{file_name} mtime as file_time "
            f"{timeobj2exif_str(file_timestamp)}")
    else:
        file_timestamp = exif_time2unix(file_time)
        logging.debug(f"{file_name} exif as file_time {file_time}")
    return file_timestamp


def compare_files(file_name_1, dir_path_1, dir_path_2, file_name_2=None):
    """Media files comparator.

    Files are matching if:
      For non-media files:
        Same size.
      For photos:
        Same exif time.
    Args:
      file_name_1: Name of the first file.
      dir_path_1: Path of the first file directory.
      dir_path_2: Path of the first file directory.
      file_name_2: Name of the first file - optional, assuming same as
      file_name_1 if None.
    Returns:
      True if files match, False otherwise.
    """
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
        logging.debug(f"{file_name_1} not a photo, match by size")
        return stat_1.st_size == stat_2.st_size

    if stat_1.st_size != stat_2.st_size:
        message = "Size diff,"
    else:
        message = "Size match,"
    if stat_1.st_mtime == stat_2.st_mtime:
        logging.debug(
            f"{file_path_1},{file_path_2} {message}, mtime match in locations")
        return True

    exif_time_1 = read_file_time(file_path_1, True)
    mtime_2 = float2timestamp(stat_2.st_mtime)
    if exif_time_1 - mtime_2 > _COMPARE_TIME_DIFF:
        logging.debug(
            f"second file {file_path_2} too old {timeobj2exif_str(mtime_2)}")
        return False
    return exif_time_1 == read_file_time(file_path_2)


class MediaFilesIterator:
    """Iterator class for MediaFiles."""
    def __init__(self, media):
        self.media = media
        self.dir_names = self.media.get_all_dir_names()
        self.dir_idx = 0
        self.dir_count = len(self.dir_names)
        self.file_idx = 0
        self._init_dir()

    def __iter__(self):
        """Iterator __iter__ method."""
        return self

    def _init_dir(self, stop_iteration=False):
        """Switching to next dir or resetting iterator."""
        if self.dir_idx < self.dir_count:
            self.cur_dir = self.media.get_dir_files(
                self.dir_names[self.dir_idx])
            self.cur_dir_count = len(self.cur_dir)
            self.file_idx = 0
        else:
            self.cur_dir = None
            self.cur_dir_count = 0
            if stop_iteration:
                raise StopIteration()

    def _next_file(self, stop_iteration=False):
        """Returns next file from current dir."""
        if self.file_idx < self.cur_dir_count:
            file_name = self.cur_dir[self.file_idx]
            self.file_idx += 1
            return (self.dir_names[self.dir_idx], file_name)
        if stop_iteration:
            raise StopIteration()
        else:
            raise IterateDir()

    def __next__(self):
        """Iterator next method."""
        try:
            return self._next_file()
        except IterateDir:
            self.dir_idx += 1
            self._init_dir(True)
            return self._next_file()


class MediaFiles:
    """Iterable representation of dir tree and files located in that tree."""
    default_types = {
        'photo': ['jpg', 'arw', 'png', 'raw'],
        'video': ['mts', 'mp4', 'mov']
    }
    storage_dirs = ['Photos', 'Videos']

    def __init__(self, root, types=None):
        self.root = root
        self.count = 0
        if types:
            self.types = types
        else:
            self.types = MediaFiles.default_types
        self.set_all_types()
        self.import_list = {}

    def __iter__(self):
        """Returns iterator."""
        return MediaFilesIterator(self)

    def set_all_types(self):
        """Flattaning self.types dictionary to list self.all_types."""
        self.all_types = []
        for _, types in self.types.items():
            self.all_types.extend(types)

    def is_importable(self, filename):
        """Checking if type of given filename is in self.all_types."""
        file_type = file_type_from_name(filename)
        return file_type in self.all_types

    def get_all_dir_names(self):
        """Returns full names of all dirs present under self.root."""
        return list(self.import_list.keys())

    def get_dir_files(self, dir_name):
        """Returns list of files of dir_name if it present under self.root."""
        if dir_name in self.import_list:
            return self.import_list[dir_name]
        return []

    def import_dir_files(self, dir_path, files_list):
        """Filter importable files from files_list and save to
           self.import_list."""
        dir_list = []
        for file_name in files_list:
            if (self.is_importable(file_name) and not (
                  Path(dir_path) / file_name).is_symlink()):
                dir_list.append(file_name)
        dir_count = len(dir_list)
        if dir_count:
            logging.debug(f"loading {dir_count} files from {dir_path}")
            self.import_list[dir_path] = dir_list
            self.count += dir_count

    def import_media(self, filter_storage=False):
        """Read dirs under self.root and import files from each dir."""
        if filter_storage:
            dir_list = MediaFiles.storage_dirs
        else:
            dir_list = ['']
        for dir_name in dir_list:
            for root, _, files in os.walk(os.path.join(self.root, dir_name)):
                self.import_dir_files(root, files)
        return self.count

    def find_file_on_media(self, file_name, file_path,
                           only_same_names=True, find_all=False):
        """Looks for file_name on current media.
          Reterning path to found copy. Order is not guaranteed.

        Args:
          file_name: name of tested file.
          file_path: full path to testd file.
          only_same_names: boolean, if True will look match only among files
            with the same name.
          find_all: boolean, if True list of all findings will be returned.

        Returns:
          Path to file on media identical to given or None if not found.
          If find_all=False list will be returned, empty list if not found."""

        copies_list = []
        for dir_path, files in self.import_list.items():
            # Skip Matching dirs to themselves, will allow import frsom subdirs
            if dir_path == file_path:
                continue
            if only_same_names:
                if (file_name in files and file_path and
                        compare_files(file_name, file_path, dir_path)):
                    found_path = os.path.join(dir_path, file_name)
                    if find_all:
                        copies_list.append(found_path)
                    else:
                        return found_path
            else:
                for storage_name in files:
                    if compare_files(
                            file_name, file_path, dir_path, storage_name):
                        found_path = os.path.join(dir_path, storage_name)
                        if find_all:
                            copies_list.append(found_path)
                        else:
                            return found_path
        return copies_list if find_all else None


def get_import_list(media_root, storage_root, filter_storage=True):
    """Generating list of files to import.

      Collecting list of files with supported types from media_root and looking
      for not yet present in storage_root

      Args:
        media_root: string, path to dir to import from
        storage_root: string, path to destination dir
        filter_storage: boolean,if True processing only MediaFiles.storage_dirs
      Returns:
        Tuple, list of files from media_root tree not found in storage_root and
        dictionary with files from media_root as keys and their matches in
        storage_root tree
    """
    media = MediaFiles(media_root)
    media.import_media()
    already_imported_files = {}
    not_imported_files = []
    logging.info(f"{media_root} contain {media.count} files")
    if not media.count:
        return (not_imported_files, already_imported_files)
    storage = MediaFiles(storage_root)
    storage.import_media(filter_storage)
    logging.info(f"{storage_root} contain {storage.count} files")
    count = 0
    present_count = 0
    for file_path, file_name in media:
        count += 1
        logging.info(f"Processing {file_name} {count}/{media.count}")
        storage_dir = storage.find_file_on_media(file_name, file_path)
        if storage_dir:
            present_count += 1
            already_imported_files[file_name] = storage_dir
            logging.info(f"{file_name} present in {storage_dir}")
        else:
            not_imported_files.append(file_name)
            logging.info(f"{file_name} NOT present in storage")
    logging.info(f"{media_root} contain {media.count} files, "
                 f"{(count - present_count)} not present in {storage_root}")
    return (not_imported_files, already_imported_files)


def print_time(path):
    """Prints time information of file or all files in dir."""
    if os.path.isdir(path):
        media = MediaFiles(path)
        media.import_media()
    else:
        media = [(os.path.split(path))]
    logging.debug(media)
    for media_file in media:
        file_path = os.path.join(media_file[0], media_file[1])
        logging.info(f"file_time({file_path})={read_file_time(file_path)}")


def import_action(args):
    """Implementation of import action."""
    if args.media is None or args.storage is None:
        logging.critical(
            "Import require both --media and --storage arguments.")
        exit(1)
    files_to_import = get_import_list(
        args.media, args.storage, filter_storage=args.import_all)
    logging.info(files_to_import)


def print_time_action(args):
    """Implementation of print_time_action action."""
    if args.media is None and args.storage is None:
        logging.critical(
            "Print_time require at least one --media or --storage argument.")
        exit(1)
    if args.media:
        print_time(args.media)
    if args.storage:
        print_time(args.storage)


def main():
    """Module as util use wrapper."""
    actions = [
        'import',
        'print_time',
    ]
    arg_parser = argparse.ArgumentParser(
        description='Import photos from media to storage')
    arg_parser.add_argument('--media', help='Media to import files from',
                            default=None)
    arg_parser.add_argument(
        '--storage', help='Storage to save imported media files',
        default=None)
    arg_parser.add_argument('--action', help='Action to perform',
                            choices=actions, default='import')
    arg_parser.add_argument(
        '--import_all',
        help='Import all files regardless of presence in storage',
        action="store_true", default=False)
    arg_parser.add_argument('-v', '--verbose',
                            help='Print verbose output',
                            action='count', default=0)
    arg_parser.add_argument('--dry_run',
                            help='Print action instead of executing it',
                            action="store_true", default=False)
    args = arg_parser.parse_args()
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))
    if args.action == 'import':
        import_action(args)
    elif args.action == 'print_time':
        print_time_action(args)


if __name__ == '__main__':
    main()
