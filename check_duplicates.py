#!/usr/bin/python3
"""Search duplicate files in database."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from file_database import DISK_SELECT, FileManagerDatabase

DEFAULT_DATABASE = Path("/var/lib/file-manager/fileManager.db")

DEFAULT_MIN_SIZE = 1000000  # 1 MB

DISK_SELECT_LABEL = ("SELECT `ROWID`, `UUID`, `DiskSize`, `Label` "
                     "FROM `disks` WHERE `Label` = ?")
DUPLICATES_FOLDERS = ("SELECT * FROM "
                      "(SELECT `FileId`, COUNT(*) AS `FileCount`, "
                      "GROUP_CONCAT(`ParentId`) AS `Parents`"
                      "FROM `fsrecords` "
                      "INNER JOIN `files` ON `files`.ROWID = `FileId` "
                      "WHERE `FileId` IS NOT NULL "
                      "AND `DiskId` = ? AND `FileSize` > ? "
                      "GROUP BY `FileId`) "
                      "WHERE `FileCount` > 1 "
                      "ORDER BY `Parents` ASC, `FileCount`")
SELECT_DIR_FILES = ("SELECT `fsrecords`.`ROWID`, `FileId`, `ParentId`, "
                    "`FileSize` "
                    "FROM `fsrecords` "
                    "INNER JOIN `files` ON `files`.`ROWID` = `FileId`"
                    "WHERE `ParentId` IN (?, ?) "
                    "ORDER BY `ParentId`, `FileId`")


class FileDuplicates(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path, min_size: int):
        super().__init__(db_path, 0)
        self._min_size = min_size

    def set_disk(self, uuid: str, size: int, label: str) -> None:
        del size
        if uuid:
            for row in self._exec_query(DISK_SELECT, (uuid,), commit=False):
                self._set_disk(row[0], row[1], row[2], row[3])
                break
            else:
                raise ValueError(
                    f"DB does not have info on disk with UUID={uuid}")
        else:
            for row in self._exec_query(
                    DISK_SELECT_LABEL, (label,), commit=False):
                self._set_disk(row[0], row[1], row[2], row[3])
                break
            else:
                raise ValueError(
                    f"DB does not have info on disk with label={label}")
        logging.info(
            f"Processing disk id={self._disk_id}, size={self._disk_size}, "
            f"label={self._disk_label}, UUID={self._disk_uuid}")

    def compare_dirs(
            self, dir_a: int, dir_b: int, max_diff: int
            ) -> Dict[str, Tuple[List[Tuple[int, int]],
                                 List[Tuple[int, int]],
                                 int]]:
        dir_a_files = []
        dir_b_files = []
        logging.debug(
            f"comparing Dir pair {self.get_path(dir_a)},{self.get_path(dir_b)}"
            f" ({dir_a},{dir_b}) for max {max_diff} diff files")
        for row in self._exec_query(SELECT_DIR_FILES,
                                    (dir_a, dir_b),
                                    commit=False):
            if int(row[2]) == int(dir_a):
                dir_a_files.append((row[1], row[3]))
            else:
                dir_b_files.append((row[1], row[3]))
        matching_size = sum([
            file[1] for file in dir_a_files if file in dir_b_files
        ])
        if dir_a_files == dir_b_files:
            logging.info(f"Dir {self.get_path(dir_a)} is matching "
                         f"{self.get_path(dir_b)} with 0 differences")
            return {f"{dir_a},{dir_b}": ([], [], matching_size)}

        a_not_b = [a for a in dir_a_files if a not in dir_b_files]
        b_not_a = [b for b in dir_b_files if b not in dir_a_files]
        result = {f"{dir_a},{dir_b}": (a_not_b, b_not_a, matching_size)}
        if not a_not_b:
            logging.info(f"Files of dir {self.get_path(dir_a)} contained "
                         f"in {self.get_path(dir_b)}")
            return result
        if not b_not_a:
            logging.info(f"Files of {self.get_path(dir_b)} "
                         f"contained in {self.get_path(dir_a)}")
            return result

        diff_count = len(a_not_b) + len(b_not_a)
        if diff_count <= max_diff:
            logging.info(
                f"Dir {self.get_path(dir_a)} is matching "
                f"{self.get_path(dir_b)} with {diff_count} differences")
            return result
        if len(a_not_b) <= max_diff:
            logging.info(f"Files of {self.get_path(dir_a)} contained in "
                         f"{self.get_path(dir_b)} with "
                         f"all but {len(a_not_b)} files")
            return result
        if len(b_not_a) <= max_diff:
            logging.info(f"Files of {self.get_path(dir_b)} contained in "
                         f"{self.get_path(dir_a)} with "
                         f"all but {len(b_not_a)} files")
            return result
        logging.debug(
            f"Dir {self.get_path(dir_a)} different from {self.get_path(dir_b)}"
            f" with {diff_count} differences")
        return {}

    def sort_output_duplicate_dirs(
            self, duplicate_dirs: Dict[str, Tuple[List[Tuple[int, int]],
                                                  List[Tuple[int, int]],
                                                  int]], max_diff: int
            ) -> None:
        sorted_duplicates = sorted(
            duplicate_dirs.keys(),
            key=lambda pair: duplicate_dirs[pair][2], reverse=True)
        for dir_pair in sorted_duplicates:
            pair = dir_pair.split(",")
            if not (duplicate_dirs[dir_pair][0] or
                    duplicate_dirs[dir_pair][1]):
                status = "full_match"
            elif not duplicate_dirs[dir_pair][0]:
                status = "left_in_right"
            elif not duplicate_dirs[dir_pair][1]:
                status = "right_in_left"
            elif (len(duplicate_dirs[dir_pair][0]) +
                  len(duplicate_dirs[dir_pair][1])) <= max_diff:
                status = f"mismatch_under_{max_diff}"
            elif len(duplicate_dirs[dir_pair][0]) <= max_diff:
                status = f"left_mismatch_right_under_{max_diff}"
            elif len(duplicate_dirs[dir_pair][1]) <= max_diff:
                status = f"right_mismatch_left_under_{max_diff}"
            else:
                status = "unexpected"

            print(f"{duplicate_dirs[dir_pair][2]} {status}: "
                  f"'{self.get_path(int(pair[0]))}' "
                  f"'{self.get_path(int(pair[1]))}'")

    def search_duplicate_folders(self, max_diff: int) -> None:
        checked_dirs = {}
        duplicate_dirs = {}
        for row in self._exec_query(DUPLICATES_FOLDERS,
                                    (self._disk_id, self._min_size),
                                    commit=False):
            logging.debug(f"File {row[0]} have {row[1]} duplicates in "
                          f"folders {row[2]}")
            dirs = sorted([int(dir) for dir in row[2].split(",")])
            for i in range(0, len(dirs)):
                for j in range(i + 1, len(dirs)):
                    if dirs[i] == dirs[j]:
                        continue
                    if (f"{dirs[i]},{dirs[j]}" in checked_dirs or
                            f"{dirs[j]},{dirs[i]}" in checked_dirs):
                        logging.debug(
                            f"Dir pair {dirs[i]},{dirs[j]} was tested before")
                        continue
                    duplicate_dirs.update(self.compare_dirs(
                        dirs[i], dirs[j], max_diff))
                    checked_dirs[f"{dirs[i]},{dirs[j]}"] = True
        self.sort_output_duplicate_dirs(duplicate_dirs, max_diff)


def main(argv):
    """Module as util use wrapper."""
    arg_parser = argparse.ArgumentParser(
        description="Search for duplicate files and folders.")
    disk = arg_parser.add_mutually_exclusive_group(required=True)
    disk.add_argument("-l", "--label", type=str,
                      help="Disk label to process")
    disk.add_argument("-u", "--uuid", type=str,
                      help="Disk UUID to process")
    arg_parser.add_argument(
        "--database",
        type=Path,
        help="Database file path",
        required=False,
        default=DEFAULT_DATABASE)
    arg_parser.add_argument(
        "-d", "--max-diff",
        type=int,
        help="Number of different files to consider as duplicate.",
        default=0)
    arg_parser.add_argument(
        "-s", "--min-size",
        type=int,
        help="Minimum size of the file to check duplicate.",
        default=DEFAULT_MIN_SIZE)
    arg_parser.add_argument("-v", "--verbose",
                            help="Print verbose output",
                            action="count", default=0)
    args = arg_parser.parse_args(argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    with FileDuplicates(args.database, args.min_size) as file_db:
        file_db.set_disk(args.uuid, 0, args.label)
        file_db.search_duplicate_folders(args.max_diff)


if __name__ == "__main__":
    main(sys.argv)
