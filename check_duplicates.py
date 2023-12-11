#!/usr/bin/python3
"""Search duplicate files in database."""

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import Any, Dict, List, Set, Tuple

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
SELECT_COPIES_IN_DIR = ("SELECT `FileId`, `ParentId`, `FileSize`,"
                        "GROUP_CONCAT(`fsrecords`.`ROWID`)"
                        "FROM `fsrecords`"
                        "INNER JOIN `files` ON `files`.`ROWID` = A.`FileId`"
                        "GROUP BY `ParentId`, `FileId`, `FileSize`"
                        "ORDER BY `ParentId`, `FileId`")


@dataclass
class DirsPair:
    DirAId: int
    DirBId: int

    def __hash__(self) -> int:
        return hash(f"{self.DirAId},{self.DirBId}")


@dataclass
class FileInfo:
    FileId: int
    FileSize: int

    def __hash__(self) -> int:
        return self.FileId


@dataclass
class DirsDifference:
    files_a_not_b: Dict[int, int]
    files_b_not_a: Dict[int, int]
    matching_size: int


class FileDuplicates(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path, min_size: int):
        super().__init__(db_path, 0)
        self._min_size = min_size
        self.compare_count = 0
        self.checked_dirs: Dict[DirsPair, bool] = {}
        self.empty_dirs: Set[int] = set()  # Dirs w/o files in any subdirs
        self.non_empty_dirs: Set[int] = set()
        self.duplicate_dirs: Dict[DirsPair, DirsDifference] = {}

    def process_profile(
            self, dir_a: int, dir_b: int, match: bool,
            start_time: int, query_time: int,
            size_time: int, dirs_files_matches_time: int,
            end_time: int, point: int,
            a_count: int, b_count: int) -> None:
        query_ms = (query_time - start_time) // 1000
        size_ms = (size_time - query_time) // 1000
        match_ms = (dirs_files_matches_time - size_time) // 1000
        compare_ms = (end_time - dirs_files_matches_time) // 1000
        qps = (a_count + b_count) * 1000 // query_ms
        mcps = max(a_count, b_count) * 1000 // (
            compare_ms + match_ms + size_ms)
        tcps = (a_count + b_count) * 1000 // (
            compare_ms + match_ms + size_ms)
        logging.debug(
            f"{dir_a}({a_count}),{dir_b}({b_count}) {match}[{point}], Times: "
            f"{query_ms}/{size_ms}/{match_ms}/{compare_ms} ms, {qps} QPS, "
            f"{mcps} CPS, {tcps} TCPS")

    def mark_as_checked(
            self, dir_a: int, dir_b: int, match: bool, profile: List[Any]
            ) -> bool:
        self.checked_dirs[DirsPair(dir_a, dir_b)] = match
        self.checked_dirs[DirsPair(dir_b, dir_a)] = match
        self.process_profile(dir_a, dir_b, match, *profile)
        return match

    def compare_subdirs(
        self, dir_a_subdirs: List[Any], dir_b_subdirs: List[Any]
    ) -> bool:
        dir_a_subdirs.sort()
        dir_b_subdirs.sort()
        return True

    def is_dir_empty(self, dir_id: int) -> bool:
        if dir_id in self.empty_dirs:
            return True
        if dir_id in self.non_empty_dirs:
            return True
        return False

    def query_files(
            self, dir_a: int, dir_b: int
            ) -> Tuple[Dict[int, int], Dict[int, int]]:
        """Queries subdirs of given pair of dirs."""
        a_file_size: Dict[int, int] = {}
        b_file_size: Dict[int, int] = {}

        for row in self._exec_query(
                SELECT_DIR_FILES, (dir_a, dir_b), commit=False):
            if int(row[2]) == int(dir_a):
                a_file_size[row[1]] = row[3]
            else:
                b_file_size[row[1]] = row[3]
        return a_file_size, b_file_size

    def query_subdirs(
            self, dir_a: int, dir_b: int
            ) -> Tuple[List[FileInfo], List[FileInfo]]:
        """Queries files of given pair of dirs."""
        dir_a_subdirs = []
        dir_b_subdirs = []
        for row in self._exec_query(
                SELECT_SUBDIRS, (dir_a, dir_b), commit=False):
            if row[1] == dir_a:
                dir_a_subdirs.append(row)
            else:
                dir_b_subdirs.append(row)
        return dir_a_subdirs, dir_b_subdirs

    def compare_dirs(self, dir_a: int, dir_b: int, max_diff: int) -> bool:
        """Compares dirs by SHA1 of files in them."""
        if DirsPair(dir_a, dir_b) in self.checked_dirs:
            logging.debug(f"Dir pair {dir_a},{dir_b} was tested before")
            return self.checked_dirs[DirsPair(dir_a, dir_b)]
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
        self.compare_count += 1

        dir_a_path = self.get_path(dir_a)
        dir_b_path = self.get_path(dir_b)
        logging.debug(
            f"comparing Dir pair {dir_a_path},{dir_b_path}"
            f" ({dir_a},{dir_b}) for max {max_diff} diff files")
        dir_a_subdirs, dir_b_subdirs = self.query_subdirs(dir_a, dir_b)
        a_file_size, b_file_size = self.query_files(dir_a, dir_b)
        query_time = clock_gettime_ns(CLOCK_MONOTONIC)

        matching_size = sum([
            a_file_size[file] for file in a_file_size if file in b_file_size
        ])
        size_time = clock_gettime_ns(CLOCK_MONOTONIC)

        dirs_files_matches: bool = (a_file_size.keys() == b_file_size.keys())
        dirs_files_matches_time = clock_gettime_ns(CLOCK_MONOTONIC)

        if dirs_files_matches:
            logging.info(f"Dir {dir_a_path} is matching "
                         f"{dir_b_path} with 0 differences")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs[DirsPair(dir_a, dir_b)] = DirsDifference(
                {}, {}, matching_size)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 1, len(a_file_size), len(b_file_size)])

        a_not_b_size: Dict[int, int] = {
            a: a_file_size[a] for a in a_file_size if a not in b_file_size}
        b_not_a_size: Dict[int, int] = {
            b: b_file_size[b] for b in b_file_size if b not in a_file_size}
        diff_count = len(a_not_b_size) + len(b_not_a_size)

        if (diff_count > max_diff and
                len(a_not_b_size) > max_diff and
                len(b_not_a_size) > max_diff):
            logging.debug(
                f"Dir {dir_a_path} different from {dir_b_path}"
                f" with {diff_count} differences")
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, False,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 2, len(a_file_size), len(b_file_size)])

        if not a_not_b_size:
            logging.info(f"Files of dir {dir_a_path} contained "
                         f"in {dir_b_path}")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs[DirsPair(dir_a, dir_b)] = DirsDifference(
                a_not_b_size, b_not_a_size, matching_size)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 3, len(a_file_size), len(b_file_size)])

        if not b_not_a_size:
            logging.info(f"Files of {dir_b_path} "
                         f"contained in {dir_a_path}")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs[DirsPair(dir_b, dir_a)] = DirsDifference(
                a_not_b_size, b_not_a_size, matching_size)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 4, len(a_file_size), len(b_file_size)])

        result = {
            DirsPair(dir_a, dir_b): DirsDifference(
                a_not_b_size, b_not_a_size, matching_size)
        }
        if diff_count <= max_diff:
            logging.info(
                f"Dir {dir_a_path} is matching "
                f"{dir_b_path} with {diff_count} differences")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 5, len(a_file_size), len(b_file_size)])
        if len(a_not_b_size) <= max_diff:
            logging.info(f"Files of {dir_a_path} contained in "
                         f"{dir_b_path} with "
                         f"all but {len(a_not_b_size)} files")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 6, len(a_file_size), len(b_file_size)])
        if len(b_not_a_size) <= max_diff:
            logging.info(f"Files of {dir_b_path} contained in "
                         f"{dir_a_path} with "
                         f"all but {len(b_not_a_size)} files")
            log_subdirs_info(
                dir_a_path, dir_b_path, dir_a_subdirs, dir_b_subdirs)
            self.duplicate_dirs.update(result)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 7, len(a_file_size), len(b_file_size)])

        logging.warning(
            f"Dir {dir_a_path} different from {dir_b_path}"
            f" with {diff_count} differences")
        end_time = clock_gettime_ns(CLOCK_MONOTONIC)
        return self.mark_as_checked(
            dir_a, dir_b, False,
            [start_time, query_time, size_time, dirs_files_matches_time,
             end_time, 8, len(a_file_size), len(b_file_size)])

    def sort_output_duplicate_dirs(self, max_diff: int) -> None:
        sorted_duplicates = sorted(
            self.duplicate_dirs.keys(),
            key=lambda pair: self.duplicate_dirs[pair].matching_size,
            reverse=True)
        for dir_pair in sorted_duplicates:
            if not (self.duplicate_dirs[dir_pair].files_a_not_b or
                    self.duplicate_dirs[dir_pair].files_b_not_a):
                status = "full_match"
            elif not self.duplicate_dirs[dir_pair].files_a_not_b:
                status = "left_in_right"
            elif not self.duplicate_dirs[dir_pair].files_b_not_a:
                status = "right_in_left"
            elif (len(self.duplicate_dirs[dir_pair].files_a_not_b) +
                  len(self.duplicate_dirs[dir_pair].files_b_not_a)
                  ) <= max_diff:
                status = f"mismatch_under_{max_diff}"
            elif len(self.duplicate_dirs[dir_pair].files_a_not_b) <= max_diff:
                status = f"left_mismatch_right_under_{max_diff}"
            elif len(self.duplicate_dirs[dir_pair].files_b_not_a) <= max_diff:
                status = f"right_mismatch_left_under_{max_diff}"
            else:
                status = "unexpected"

            print(f"{self.duplicate_dirs[dir_pair].matching_size} B {status}: "
                  f"'{self.get_path(dir_pair.DirAId)}' "
                  f"'{self.get_path(dir_pair.DirBId)}'")

    def search_duplicate_folders(self, max_diff: int) -> None:
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
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
        query_time = clock_gettime_ns(CLOCK_MONOTONIC)
        self.sort_output_duplicate_dirs(max_diff)
        sort_time = clock_gettime_ns(CLOCK_MONOTONIC)
        logging.info(f"Query time {query_time - start_time} ns, "
                     f"Sort time {sort_time - query_time}, "
                     f"Compare_count={self.compare_count}, "
                     f"checked_dirs_count={len(self.checked_dirs)}, "
                     f"duplicate_dirs_count={len(self.duplicate_dirs)}, "
                     )


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
