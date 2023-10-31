#!/usr/bin/python3
"""Utils to view/analize file database."""
import argparse
import logging
from pathlib import Path
from typing import Callable, List

from file_database import DEFAULT_DATABASE, FileManagerDatabase
from utils import print_table, timestamp2exif_str

_DISKS_SELECT = "SELECT `ROWID`, `UUID`, `Label`, `DiskSize` FROM `disks`"

_DIR_LIST_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`Name`, "
                    "`fsrecords`.`FileDate`, `fsrecords`.`SHA1ReadDate`, "
                    "`files`.`ROWID`, `files`.`FileSize`, `SHA1`"
                    " FROM `fsrecords` LEFT JOIN `files`"
                    " ON `files`.`ROWID` = `fsrecords`.`FileId`"
                    " WHERE `ParentId` = ? ORDER BY `FileId`, `Name`")


class FileUtils(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path):
        super().__init__(db_path, 0)

    def list_disks(self, filter: str) -> None:
        disks = []
        for row in self._exec_query(_DISKS_SELECT, (), commit=False):
            if filter and filter not in row[1:3]:
                continue
            disks.append(row)
        headers = ["DiskID", "UUID", "Label", "DiskSize"]
        print_table(disks, headers)

    def list_dir(self, disk: str, dir_path: str) -> None:
        self.set_disk_by_name(disk)
        self._cur_dir_id = self.get_dir_id(
            dir_path.split("/"), insert_dirs=False)
        logging.debug(
            f"Listing dir {self.disk_name}/{dir_path} id={self._cur_dir_id}")
        dir_content = []
        for row in self._exec_query(
                _DIR_LIST_SELECT, (self._cur_dir_id,), commit=False):
            dir_content.append(row)
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
        print_table(
            dir_content, headers, indexes=indexes,
            formats=formats, aligns=aligns)


def list_disks_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing disks."""
    file_db.list_disks(args.disk)
    return 0


def list_dir_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing directory."""
    file_db.list_dir(args.disk, args.dir_path)
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
        help="Disk label or UUID to process, requirted for list-dir")
    subparsers = arg_parser.add_subparsers(
        help="Commands supported by CLI tool", dest="command")

    list_disks = subparsers.add_parser(
        "list-disks", help="List disks with statistic")
    list_disks.set_defaults(func=list_disks_command)
    list_disks.add_argument(
        "-s", "--size", help="Calculate used space", action="store_true")

    list_dir = subparsers.add_parser(
        "list-dir", help="List directory with statistic")
    list_dir.set_defaults(func=list_dir_command)
    list_dir.add_argument("dir_path", type=str, help="Path to dir to list")
    list_disks.add_argument(
        "-r", "--recursive", help="List dir recursively", action="store_true")

    args = arg_parser.parse_args()
    if hasattr(args, "dir_path") and not args.disk:
        arg_parser.error("-d DISK argument is required for list-dir")
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
