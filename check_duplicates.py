#!/usr/bin/python3
"""Search duplicate files in database."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from file_database import DEFAULT_DATABASE, FileManagerDatabase

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
SELECT_SUBDIRS = ("SELECT `ROWID`, `Name`, `ParentId` FROM `fsrecords` "
                  "WHERE `ParentId` IN (?, ?) AND `FileId` IS NULL "
                  "ORDER BY `ParentId`, `Name`")


class FileDuplicates(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path, min_size: int):
        super().__init__(db_path, 0)
        self._min_size = min_size
        self.checked_dirs: Dict[str, bool] = {}
        self.duplicate_dirs: Dict[str, Tuple[List[Tuple[int, int]],
                                             List[Tuple[int, int]],
                                             int]] = {}

    def mark_as_checked(self, dir_a: int, dir_b: int, match: bool) -> bool:
        self.checked_dirs[f"{dir_a},{dir_b}"] = match
        self.checked_dirs[f"{dir_b},{dir_a}"] = match
        return match

    def compare_dirs(self, dir_a: int, dir_b: int, max_diff: int) -> bool:
        if f"{dir_a},{dir_b}" in self.checked_dirs:
            logging.debug(f"Dir pair {dir_a},{dir_b} was tested before")
            return self.checked_dirs[f"{dir_b},{dir_a}"]
        dir_a_files = []
        dir_b_files = []
        dir_a_subdirs = []
        dir_b_subdirs = []
        for row in self._exec_query(
                SELECT_SUBDIRS, (dir_a, dir_b), commit=False):
            if row[1] == dir_a:
                dir_a_subdirs.append(row)
            else:
                dir_b_subdirs.append(row)

        dir_a_path = self.get_path(dir_a)
        dir_b_path = self.get_path(dir_b)
        logging.debug(
            f"comparing Dir pair {dir_a_path},{dir_b_path}"
            f" ({dir_a},{dir_b}) for max {max_diff} diff files")
        for row in self._exec_query(
                SELECT_DIR_FILES, (dir_a, dir_b), commit=False):
            if int(row[2]) == int(dir_a):
                dir_a_files.append((row[1], row[3]))
            else:
                dir_b_files.append((row[1], row[3]))
        matching_size = sum([
            file[1] for file in dir_a_files if file in dir_b_files
        ])

        if dir_a_files == dir_b_files:
            logging.info(f"Dir {dir_a_path} is matching "
                         f"{dir_b_path} with 0 differences")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            result: Dict[
                str,
                Tuple[List[Tuple[int, int]],
                      List[Tuple[int, int]],
                      int]] = {f"{dir_a},{dir_b}": ([], [], matching_size)}
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)

        a_not_b = [a for a in dir_a_files if a not in dir_b_files]
        b_not_a = [b for b in dir_b_files if b not in dir_a_files]
        result = {f"{dir_a},{dir_b}": (a_not_b, b_not_a, matching_size)}
        if not a_not_b:
            logging.info(f"Files of dir {dir_a_path} contained "
                         f"in {dir_b_path}")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)
        if not b_not_a:
            logging.info(f"Files of {dir_b_path} "
                         f"contained in {dir_a_path}")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)

        diff_count = len(a_not_b) + len(b_not_a)
        if diff_count <= max_diff:
            logging.info(
                f"Dir {dir_a_path} is matching "
                f"{dir_b_path} with {diff_count} differences")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)
        if len(a_not_b) <= max_diff:
            logging.info(f"Files of {dir_a_path} contained in "
                         f"{dir_b_path} with "
                         f"all but {len(a_not_b)} files")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)
        if len(b_not_a) <= max_diff:
            logging.info(f"Files of {dir_b_path} contained in "
                         f"{dir_a_path} with "
                         f"all but {len(b_not_a)} files")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            return self.mark_as_checked(dir_a, dir_b, True)
        logging.debug(
            f"Dir {dir_a_path} different from {dir_b_path}"
            f" with {diff_count} differences")
        return self.mark_as_checked(dir_a, dir_b, False)

    def sort_output_duplicate_dirs(self, max_diff: int) -> None:
        sorted_duplicates = sorted(
            self.duplicate_dirs.keys(),
            key=lambda pair: self.duplicate_dirs[pair][2], reverse=True)
        for dir_pair in sorted_duplicates:
            pair = dir_pair.split(",")
            if not (self.duplicate_dirs[dir_pair][0] or
                    self.duplicate_dirs[dir_pair][1]):
                status = "full_match"
            elif not self.duplicate_dirs[dir_pair][0]:
                status = "left_in_right"
            elif not self.duplicate_dirs[dir_pair][1]:
                status = "right_in_left"
            elif (len(self.duplicate_dirs[dir_pair][0]) +
                  len(self.duplicate_dirs[dir_pair][1])) <= max_diff:
                status = f"mismatch_under_{max_diff}"
            elif len(self.duplicate_dirs[dir_pair][0]) <= max_diff:
                status = f"left_mismatch_right_under_{max_diff}"
            elif len(self.duplicate_dirs[dir_pair][1]) <= max_diff:
                status = f"right_mismatch_left_under_{max_diff}"
            else:
                status = "unexpected"

            print(f"{self.duplicate_dirs[dir_pair][2]} B {status}: "
                  f"'{self.get_path(int(pair[0]))}' "
                  f"'{self.get_path(int(pair[1]))}'")

    def search_duplicate_folders(self, max_diff: int) -> None:
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
                    self.compare_dirs(dirs[i], dirs[j], max_diff)
        self.sort_output_duplicate_dirs(max_diff)


def log_subdirs_info(
        dir_a_path: str, dir_b_path: str,
        dir_a_subdirs: List[Any], dir_b_subdirs: List[Any]
) -> None:
    if dir_a_subdirs:
        logging.info(f"Dir {dir_a_path} have subdirs: {dir_a_subdirs}")
    if dir_b_subdirs:
        logging.info(f"Dir {dir_b_path} have subdirs: {dir_b_subdirs}")


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
        file_db.set_disk_by_name(args.uuid or args.label)
        file_db.search_duplicate_folders(args.max_diff)


if __name__ == "__main__":
    main(sys.argv)
