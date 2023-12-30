#!/usr/bin/python3
"""Search duplicate files in database."""

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import OperationalError
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import Any, Dict, List, Tuple

from duplicates_cleanup import (ALLOWED_ACTIONS, LEFT_DIR_KEEP_ACTION,
                                RIGHT_DIR_KEEP_ACTION, SKIP_ACTION,
                                DuplicatesCleanup)
from file_database import DEFAULT_DATABASE, FileManagerDatabase
from file_manager import get_disk_info
from file_utils import generate_file_sha1
from utils import print_table

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
                    "`FileSize`, `fsrecords`.`Name`, `files`.`SHA1` "
                    "FROM `fsrecords` "
                    "INNER JOIN `files` ON `files`.`ROWID` = `FileId`"
                    "WHERE `ParentId` IN (?, ?) "
                    "ORDER BY `ParentId`, `FileId`")
SELECT_COPIES_IN_DIR = ("SELECT *, `FileSize`*(duplicates_count-1) AS dup_size"
                        " FROM (SELECT `FileId`, `ParentId`, `FileSize`, "
                        "GROUP_CONCAT(`fsrecords`.`ROWID`) AS records, "
                        "COUNT(*) AS duplicates_count, `SHA1` "
                        "FROM `fsrecords` "
                        "INNER JOIN `files` ON `files`.`ROWID` = `FileId` "
                        "WHERE `DiskId` = ? AND `FileSize` > ? "
                        "GROUP BY `ParentId`, `FileId`, `FileSize`) "
                        "WHERE duplicates_count > 1 "
                        "ORDER BY dup_size DESC, `ParentId`, `FileId`")
DELETE_FSRECORD = "DELETE FROM `fsrecords` WHERE `ROWID` = ?"


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
    FileSHA1: str

    def __hash__(self) -> int:
        return self.FileId


@dataclass
class FSRecortInfo:
    FSRecordId: int
    Name: str
    ParentId: int
    FileId: int


@dataclass
class DirsDifference:
    files_a_not_b: Dict[int, int]
    files_b_not_a: Dict[int, int]
    matching_size: int
    common_files: Dict[int, int]


class FileDuplicates(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path, min_size: int, rehash: bool,
            cleanup_config: DuplicatesCleanup, cleanup: bool, dry_run: bool,
            auto_execute_default: bool
            ):
        super().__init__(db_path, 0)
        self._min_size = min_size
        self.compare_count = 0
        self.checked_dirs: Dict[DirsPair, bool] = {}
        self.duplicate_dirs: Dict[DirsPair, DirsDifference] = {}
        self.mountpoint: Path = Path(".")
        self.file_info: Dict[int, FileInfo] = {}
        self.fsrecord_info: Dict[int, FSRecortInfo] = {}
        self.dir_fsrecords: Dict[int, List[int]] = {}
        self.rehash = rehash
        self.checked_hashes: Dict[int, str] = {}
        self.cleanup_config = cleanup_config
        self.cleanup = cleanup
        self.dry_run = dry_run
        self.auto_execute_default = auto_execute_default

    def set_mountpoint(self):
        """Request lsblk disk info, raises ValueError if not mounted."""
        self.mountpoint = Path(get_disk_info(self._disk_uuid)["mountpoint"])

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

    def query_files(
            self, dir_a: int, dir_b: int
            ) -> Tuple[Dict[int, int], Dict[int, int]]:
        """Queries files of given pair of dirs."""
        a_file_size: Dict[int, int] = {}
        b_file_size: Dict[int, int] = {}
        self.dir_fsrecords[dir_a] = []
        self.dir_fsrecords[dir_b] = []

        for row in self._exec_query(
                SELECT_DIR_FILES, (dir_a, dir_b), commit=False):
            self.file_info[row[1]] = FileInfo(row[1], row[3], row[5])
            self.fsrecord_info[row[0]] = FSRecortInfo(
                row[0], row[4], row[2], row[1])
            if int(row[2]) == int(dir_a):
                a_file_size[row[1]] = row[3]
                self.dir_fsrecords[dir_a].append(row[0])
            else:
                b_file_size[row[1]] = row[3]
                self.dir_fsrecords[dir_b].append(row[0])
        return a_file_size, b_file_size

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
        a_file_size, b_file_size = self.query_files(dir_a, dir_b)
        query_time = clock_gettime_ns(CLOCK_MONOTONIC)

        matching_files = {a_file_id: a_file_size[a_file_id]
                          for a_file_id in a_file_size
                          if a_file_id in b_file_size}
        matching_size = sum(matching_files.values())
        size_time = clock_gettime_ns(CLOCK_MONOTONIC)

        dirs_files_matches: bool = (a_file_size.keys() == b_file_size.keys())
        dirs_files_matches_time = clock_gettime_ns(CLOCK_MONOTONIC)

        if dirs_files_matches:
            logging.info(f"Dir {dir_a_path} is matching "
                         f"{dir_b_path} with 0 differences")
            self.duplicate_dirs[DirsPair(dir_a, dir_b)] = DirsDifference(
                {}, {}, matching_size, matching_files)
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
            self.duplicate_dirs[DirsPair(dir_a, dir_b)] = DirsDifference(
                a_not_b_size, b_not_a_size, matching_size, matching_files)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 3, len(a_file_size), len(b_file_size)])

        if not b_not_a_size:
            logging.info(f"Files of {dir_b_path} "
                         f"contained in {dir_a_path}")
            self.duplicate_dirs[DirsPair(dir_b, dir_a)] = DirsDifference(
                b_not_a_size, a_not_b_size, matching_size, matching_files)
            end_time = clock_gettime_ns(CLOCK_MONOTONIC)
            return self.mark_as_checked(
                dir_a, dir_b, True,
                [start_time, query_time, size_time, dirs_files_matches_time,
                 end_time, 4, len(a_file_size), len(b_file_size)])

        result = {
            DirsPair(dir_a, dir_b): DirsDifference(
                a_not_b_size, b_not_a_size, matching_size, matching_files)
        }
        if diff_count <= max_diff:
            logging.info(
                f"Dir {dir_a_path} is matching "
                f"{dir_b_path} with {diff_count} differences")
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
        """"Sorts and prints dound duplicates, triggers cleanup if needed."""
        sorted_duplicates = sorted(
            self.duplicate_dirs.keys(),
            key=lambda pair: self.duplicate_dirs[pair].matching_size,
            reverse=True)
        reclaimed_size = 0
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
            if self.cleanup:
                reclaimed_size += self.dirs_pair_cleanup(dir_pair)
        if self.cleanup:
            print(f"{reclaimed_size}B was reclaimed")

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

    def search_file_copies_in_folder(self) -> None:
        reclaim_size = 0
        reclaimed_size = 0
        reclaim_groups = 0
        files_to_delete = 0
        for row in self._exec_query(SELECT_COPIES_IN_DIR,
                                    (self._disk_id, self._min_size),
                                    commit=False):
            self.file_info[row[0]] = FileInfo(row[0], row[2], row[5])
            reclaim_size += row[6]
            reclaim_groups += 1
            files_to_delete += row[4] - 1
            print(
                f"Folder {self.get_path(row[1])} have group of {row[4]} same "
                f"by content files, size to reclaim {row[6]}B, {row[2]}B each:"
            )
            files_counter = 0
            fsrecords_list = row[3].split(",")
            for fs_record_id in fsrecords_list:
                fs_record_id = int(fs_record_id)
                file_path = self.get_path(fs_record_id)
                file_name = file_path.replace(f"{self.get_path(row[1])}/", "")
                self.fsrecord_info[fs_record_id] = FSRecortInfo(
                    fs_record_id, file_name, row[1], row[0])
                files_counter += 1
                print(f"{files_counter}: {file_path}")
            if self.cleanup:
                reclaimed_size += self.in_folder_cleanup_action(fsrecords_list)
        print(f"In-folders duplicates: {reclaim_groups} groups,",
              f"{files_to_delete} files to delete, {reclaim_size}B to reclaim",
              f"{reclaimed_size}B was reclaimed" if self.cleanup else "")

    def in_folder_cleanup_action(self, fsrecord_id_list: List[str]) -> int:
        keep_idx = -1
        duplicates_count = len(fsrecord_id_list)
        keep_idx = 0 if self.auto_execute_default else -1
        while keep_idx < 0 or keep_idx > duplicates_count:
            try:
                value = input("Select file to keep, 0 to skip clenup: [0]")
                if value == "":
                    return 0
                keep_idx = int(value)
            except ValueError:
                print(f"{value} is not a correct index, please enter number "
                      f"0..{duplicates_count}")
        if not keep_idx:
            return 0
        keep_fsrecord_id = int(fsrecord_id_list[keep_idx - 1])
        file_path = self.mountpoint / self.get_path(keep_fsrecord_id)
        if not (file_path.exists() and self.check_hash(keep_fsrecord_id)):
            raise ValueError(
                f"Selected to keep {file_path} is missing/different on disk.")
        delete_list: List[int] = []
        for idx in range(duplicates_count):
            if idx + 1 == keep_idx:
                continue
            delete_list.append(int(fsrecord_id_list[idx]))
        return self.delete_files(delete_list)

    def dirs_pair_cleanup(self, pair: DirsPair) -> int:
        dir_a_path = self.get_path(pair.DirAId)
        dir_b_path = self.get_path(pair.DirBId)
        print(f"Matching files in {dir_a_path} and {dir_b_path}:")
        compare_table = []
        common_files: Dict[int, DirsPair] = {}
        # compare_table.extend(self.get_dir_only(pair, pair.DirAId))
        for fs_record_a_id in self.dir_fsrecords[pair.DirAId]:
            fsrecord_info = self.fsrecord_info[fs_record_a_id]
            if fsrecord_info.FileId in self.duplicate_dirs[pair].common_files:
                fsrecord_info_b = None
                for fs_record_b_id in self.dir_fsrecords[pair.DirBId]:
                    fsrecord_info_b = self.fsrecord_info[fs_record_b_id]
                    if fsrecord_info_b.FileId == fsrecord_info.FileId:
                        common_files[fsrecord_info.FileId] = DirsPair(
                            fs_record_a_id, fs_record_b_id
                        )
                        break
                else:
                    raise ValueError(
                        f"Failed to find match in dir {pair.DirBId} for"
                        "fsrecord {fs_record_a_id} from dir {pair.DirAId}")

                compare_table.append([
                    fsrecord_info.Name,
                    self.file_info[fsrecord_info.FileId].FileSHA1[:8],
                    fsrecord_info_b.Name
                ])
        # compare_table.extend(self.get_dir_only(pair, pair.DirBId))
        print_table(compare_table, [dir_a_path, "Short SHA", dir_b_path],
                    aligns=["<", ">", "<"], footer=True)
        default_action = self.cleanup_config.select_dir_to_keep(
            dir_a_path, dir_b_path)
        return self.dirs_cleanup_action(common_files, default_action)

    def get_dir_only(self, pair: DirsPair, dir_id: int) -> List[List[str]]:
        """Processing pair to extract files only in given."""
        compare_table = []
        for fs_record_id in self.dir_fsrecords[dir_id]:
            fsrecord_info = self.fsrecord_info[fs_record_id]
            only_dir_files = (self.duplicate_dirs[pair].files_a_not_b
                              if dir_id == pair.DirAId else
                              self.duplicate_dirs[pair].files_b_not_a)
            if fsrecord_info.FileId in only_dir_files:
                compare_table.append([
                    fsrecord_info.Name if dir_id == pair.DirAId else "",
                    self.file_info[fsrecord_info.FileId].FileSHA1[:8],
                    fsrecord_info.Name if dir_id == pair.DirBId else "",
                ])
        return compare_table

    def dirs_cleanup_action(self, common_files: Dict[int, DirsPair],
                            default_action: str) -> int:
        action = default_action if self.auto_execute_default else ""
        while action not in ALLOWED_ACTIONS:
            action = input(
                f"Enter action ({SKIP_ACTION}-skip/"
                f"{LEFT_DIR_KEEP_ACTION}-keep left, remove right/"
                f"{RIGHT_DIR_KEEP_ACTION}-keep right, remove left) "
                f"[{default_action}]: "
            )
            if not action:
                action = default_action
        if action == SKIP_ACTION:
            return 0
        remove_list = []
        for file_pair in common_files.values():
            if action == LEFT_DIR_KEEP_ACTION:  # keep left, remove right
                keep_id = file_pair.DirAId
                delete_id = file_pair.DirBId
            elif action == RIGHT_DIR_KEEP_ACTION:  # keep right, remove left
                keep_id = file_pair.DirBId
                delete_id = file_pair.DirAId
            else:
                raise ValueError(f"Unexpected action {action}")
            if not ((self.mountpoint / self.get_path(keep_id)).exists()
                    and self.check_hash(keep_id)):
                logging.warning(
                    "Failed to locate file "
                    f"{self.fsrecord_info[keep_id].Name} in dir "
                    f"{self.get_path(self.fsrecord_info[keep_id].ParentId)}, "
                    "skipped deletion of "
                    f"{self.fsrecord_info[delete_id].Name} from dir"
                    f" {self.get_path(self.fsrecord_info[delete_id].ParentId)}"
                )
            else:
                remove_list.append(delete_id)
        return self.delete_files(remove_list)

    def delete_files(self, fsrecord_id_list: List[int]) -> int:
        """Returns size of reclamed space."""
        deleted_size = 0
        for id in fsrecord_id_list:
            file_path = self.mountpoint / self.get_path(id)
            if file_path.exists() and self.check_hash(id):
                print(f"Deleting {file_path}",
                      "dry-run" if self.dry_run else "")
                deleted_size += file_path.stat().st_size
                if not self.dry_run:
                    file_path.unlink()

            if not self.dry_run:
                self.delete_fsrecord(id)
        return deleted_size

    def check_hash(self, fsrecord_id: int) -> bool:
        """Checks hash if self.rehash, otherwise returns true"""
        if not self.rehash:
            return True
        disk_hash = self.get_file_hash(fsrecord_id)
        file_info = self.file_info[self.fsrecord_info[fsrecord_id].FileId]
        if disk_hash != file_info.FileSHA1:
            logging.warning(
                f"DB SHA1 of fsrecord {fsrecord_id} {file_info.FileSHA1} is "
                f"not matching SHA1 {disk_hash} of "
                f"{self.mountpoint / self.get_path(fsrecord_id)}")
        return disk_hash == file_info.FileSHA1

    def get_file_hash(self, fsrecord_id: int) -> str:
        if fsrecord_id in self.checked_hashes:
            return self.checked_hashes[fsrecord_id]
        self.checked_hashes[fsrecord_id], _ = generate_file_sha1(
                self.mountpoint / self.get_path(fsrecord_id))
        return self.checked_hashes[fsrecord_id]

    def delete_fsrecord(self, id: int) -> bool:
        try:
            self._exec_query(DELETE_FSRECORD, (id,), commit=True)
        except OperationalError:
            return False
        return True


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
    arg_parser.add_argument("-c", "--cleanup",
                            help="Run inteructive cleanup mode",
                            action="store_true")
    arg_parser.add_argument("--cleanup-config",
                            help="Config for cleanup automation",
                            type=Path)
    arg_parser.add_argument("-y", "--yes",
                            help="Accept all default actions",
                            action="store_true")
    arg_parser.add_argument("--hash",
                            help="Re-hash files before deletion",
                            action="store_true")
    arg_parser.add_argument("--dry-run",
                            help="Do not delete duplicates, run all checks",
                            action="store_true")
    arg_parser.add_argument("-v", "--verbose",
                            help="Print verbose output",
                            action="count", default=0)
    args = arg_parser.parse_args(argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    cleanup_config = DuplicatesCleanup(args.cleanup_config)
    with FileDuplicates(
            args.database, args.min_size, args.hash, cleanup_config,
            args.cleanup, args.dry_run, args.yes
            ) as file_db:
        file_db.set_disk_by_name(args.uuid or args.label)
        if args.cleanup:
            file_db.set_mountpoint()
        file_db.search_file_copies_in_folder()
        file_db.search_duplicate_folders(args.max_diff)


if __name__ == "__main__":
    main(sys.argv)
