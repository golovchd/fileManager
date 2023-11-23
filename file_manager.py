#!/usr/bin/python3
"""Utils to view/analize file database."""
import argparse
import logging
from pathlib import Path
from typing import Callable, List, Tuple

from file_database import DEFAULT_DATABASE, FileManagerDatabase
from file_utils import get_disk_info
from utils import print_table, timestamp2exif_str

_DISKS_SELECT = "SELECT `ROWID`, `UUID`, `Label`, `DiskSize`/1024 FROM `disks`"
_DISK_SELECT_SIZE = ("SELECT `disks`.`ROWID`, `UUID`, `Label`, "
                     "`DiskSize`/1024, SUM(`FileSize`)/1048576 FROM `disks`"
                     "INNER JOIN `fsrecords` ON `DiskId` = `disks`.`ROWID` "
                     "INNER JOIN `files` ON `files`.`ROWID` = `FileId` "
                     "WHERE `disks`.`ROWID` IN ({})"
                     "GROUP BY `disks`.`ROWID` "
                     "ORDER BY `disks`.`ROWID`")
_DISK_UPDATE_SIZE = ("UPDATE `disks` SET `DiskSize` = ?, `Label` = ?"
                     " WHERE `ROWID` = ?")
_DIR_LIST_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`Name`, "
                    "`fsrecords`.`FileDate`, `fsrecords`.`SHA1ReadDate`, "
                    "`files`.`ROWID`, `files`.`FileSize`, `SHA1`"
                    " FROM `fsrecords` LEFT JOIN `files`"
                    " ON `files`.`ROWID` = `fsrecords`.`FileId`"
                    " WHERE `ParentId` = ? ORDER BY `FileId`, `Name`")
_UNIQUE_FILES_SIZE = "SELECT SUM(`FileSize`)/1048576 FROM `files`"
_UNIQUE_FILES_SIZE_DISKS = ("SELECT `disks`.`ROWID`, `UUID`, `Label`, "
                            "`DiskSize`/1024, SUM(`Size`)/1048576 FROM `disks`"
                            "INNER JOIN "
                            "(SELECT `DiskId`, `FileId`, "
                            "MIN(`FileSize`) AS `Size` FROM `files` "
                            "INNER JOIN `fsrecords` "
                            "ON `files`.`ROWID` = `fsrecords`.`FileId` "
                            "WHERE `DiskId` IN ({}) "
                            "GROUP BY `DiskId`, `FileId`) AS `unique_files`"
                            "ON `unique_files`.`DiskId` = `disks`.`ROWID` "
                            "GROUP BY `disks`.`ROWID` "
                            "ORDER BY `disks`.`ROWID`")


class FileUtils(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path):
        super().__init__(db_path, 0)

    def query_disks(
            self, filter: str,
            size_query: str = _DISK_SELECT_SIZE,
            cal_size: bool = False,
            id_list: Tuple[int, ...] = ()) -> List[List[str]]:
        query = (size_query.format("?" + ",?" * (len(id_list) - 1))
                 if cal_size else _DISKS_SELECT)
        disks = []
        for row in self._exec_query(
                query, id_list if id_list else (), commit=False):
            if filter and filter not in row[1:3]:
                continue
            row = list(row)
            if cal_size:
                row.append(round(100 * row[-1] / row[-2], 2))
            disks.append(row)
        return disks

    def list_disks(self, filter: str, cal_size: bool) -> None:
        disks_list = self.query_disks(filter)
        if not disks_list:
            logging.warning(f"Filter {filter} does not match any disk")
            return
        headers = ["DiskID", "UUID", "Label", "DiskSize, MiB"]
        if not cal_size:
            print_table(disks_list, headers)
            return

        headers.extend(["FilesSize, MiB", "Usage, %"])
        print_table(self.query_disks(
            "", cal_size=True,
            id_list=tuple(int(disk[0]) for disk in disks_list)), headers)

    def update_disk(self, filter: str) -> None:
        disks_list = self.query_disks(filter)
        if len(disks_list) > 1:
            raise ValueError(f"More then one disk is matching UUID {filter}")
        disk_info = get_disk_info(disks_list[0][1])
        self._exec_query(
            _DISK_UPDATE_SIZE,
            (int(disk_info["fssize"]) // 1024,
             disk_info["label"],
             disks_list[0][0]),
            commit=True)

    def unique_files(self, filter: str) -> None:
        disks_list = self.query_disks(filter)
        if not disks_list:
            logging.warning(f"Filter {filter} does not match any disk")
            return
        id_list = tuple(int(disk[0]) for disk in disks_list)
        disk_usage = self.query_disks("", cal_size=True, id_list=id_list)
        unique_files = self.query_disks(
            "", size_query=_UNIQUE_FILES_SIZE_DISKS,
            cal_size=True, id_list=id_list)
        for index in range(len(unique_files)):
            unique_files[index].insert(4, disk_usage[index][4])
            unique_files[index].insert(-1, disk_usage[index][-1])
            unique_files[index].append(str(round(
                100 * int(unique_files[index][5]) /
                int(unique_files[index][4]),
                2)))

        headers = [
            "DiskID", "UUID", "Label", "DiskSize, MiB", "FilesSize, MiB",
            "UniqueFiles, MiB", "Usage, %", "Unique Usage, %", "Unique %",
        ]
        print_table(unique_files, headers)

        for row in self._exec_query(_UNIQUE_FILES_SIZE, (), commit=False):
            print(f"Total size of unique files is {row[0]} MiB")

    def list_dir(
                self, disk: str, dir_path: str, recursive: bool,
                summary: bool = False, only_count: bool = False
            ) -> Tuple[int, int, int]:
        self.set_disk_by_name(disk)
        self._cur_dir_id = self.get_dir_id(
            dir_path.split("/"), insert_dirs=False)
        logging.debug(
            f"Listing dir {self.disk_name}/{dir_path} id={self._cur_dir_id}")
        dir_content = []
        dir_size = 0
        files_count = 0
        subdir_count = 0
        for row in self._exec_query(
                _DIR_LIST_SELECT, (self._cur_dir_id,), commit=False):
            dir_content.append(row)
            print(dir_content[-1])
            dir_size += int(row[5]) if row[5] else 0
            if row[5]:
                files_count += 1
            else:
                subdir_count += 1

        if not (summary or only_count):
            print_dir_content(dir_path, dir_content)

        if not only_count and (not recursive or subdir_count and not summary):
            suffix = " (not counted)" if subdir_count else ""
            print(f"Size of files in {dir_path} is {dir_size}B, contains "
                  f"{files_count} files and {subdir_count} subdirs {suffix}")
        if not recursive:
            return dir_size, files_count, subdir_count

        for record in dir_content:
            if record[5] is not None:
                continue
            subdir_size, subdir_files_count, subdir_dirs_count = self.list_dir(
                disk, f"{dir_path}/{record[1]}", True,
                only_count=summary or only_count)
            dir_size += subdir_size
            files_count += subdir_files_count
            subdir_count += subdir_dirs_count
        if not only_count:
            print(f"Size of files in {dir_path} with subdirs is {dir_size} B, "
                  f"contains {files_count} files and {subdir_count} subdirs")
        return dir_size, files_count, subdir_count


def print_dir_content(dir_path: str, dir_content: List[List[str]]) -> None:
    headers = ["Name", "Size", "File Date", "Hash Date, SHA256"]
    indexes = [1, 5, 2, 3, 6]
    formats: List[Callable] = [
        str,
        lambda x: str(x) if x else "dir",
        timestamp2exif_str,
        timestamp2exif_str,
        str
    ]
    aligns = ["<", ">", ">", ">", "<"]
    print(f"Listing dir: {dir_path}")
    print_table(
        dir_content, headers, indexes=indexes,
        formats=formats, aligns=aligns)


def list_disks_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing disks."""
    file_db.list_disks(args.disk, args.size)
    return 0


def list_dir_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing directory."""
    file_db.list_dir(
        args.disk, args.dir_path, args.recursive, summary=args.summary)
    return 0


def update_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.update_disk(args.disk)
    return 0


def unique_files_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.unique_files(args.disk)
    return 0


def parse_arguments() -> argparse.Namespace:
    """CLI arguments parser."""
    arg_parser = argparse.ArgumentParser(
        description="Utility to view/analize file database.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    arg_parser.add_argument(
        "-v", "--verbose", help="Verbose output", action="store_true")
    arg_parser.add_argument(
        "--database",
        type=Path,
        help="Database file path",
        required=False,
        default=DEFAULT_DATABASE)
    arg_parser.add_argument(
        "-d", "--disk", type=str,
        help=("Disk label or UUID to process, requirted for list-dir, "
              "update-disk"))
    subparsers = arg_parser.add_subparsers(
        help="Commands supported by CLI tool", dest="command")

    list_disks = subparsers.add_parser(
        "list-disks", help="List disks with statistic")
    list_disks.set_defaults(func=list_disks_command, cmd_name="list-disks")
    list_disks.add_argument(
        "-s", "--size", help="Calculate space used by files",
        action="store_true")

    list_dir = subparsers.add_parser(
        "list-dir", help="List directory with statistic")
    list_dir.set_defaults(func=list_dir_command, cmd_name="list-dir")
    list_dir.add_argument("dir_path", type=str, help="Path to dir to list")
    list_dir.add_argument(
        "-r", "--recursive", help="List dir recursively", action="store_true")
    list_dir.add_argument(
        "-s", "--summary", help="Print only summary", action="store_true")

    update_disk = subparsers.add_parser(
        "update-disk", help="Update disk with given UUID")
    update_disk.set_defaults(func=update_disk_command, cmd_name="update-disk")

    unique_files = subparsers.add_parser(
        "unique-files", help="Calculate size of unique files")
    unique_files.set_defaults(
        func=unique_files_command, cmd_name="unique-files")
    unique_files.add_argument(
        "-s", "--sort", help="Print only summary", action="store_true")

    args = arg_parser.parse_args()
    if args.cmd_name == "list-dir" and not args.disk:
        arg_parser.error("-d DISK argument is required for list-dir")
    if args.cmd_name == "update-disk" and not args.disk:
        arg_parser.error("-d DISK argument is required for update-disk")
    return args


def main() -> int:
    """CLI executor."""
    args = parse_arguments()
    lvl = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=lvl, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.debug(args)
    with FileUtils(args.database) as file_db:
        return args.func(file_db, args)


if __name__ == '__main__':
    exit(main())
