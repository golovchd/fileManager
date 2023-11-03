#!/usr/bin/python3
"""Utils to view/analize file database."""
import argparse
import logging
from pathlib import Path
from typing import Callable, List

from file_database import DEFAULT_DATABASE, FileManagerDatabase
from file_utils import get_disk_info
from utils import print_table, timestamp2exif_str

_DISKS_SELECT = "SELECT `ROWID`, `UUID`, `Label`, `DiskSize` FROM `disks`"
_DISK_UPDATE_SIZE = ("UPDATE `disks` SET `DiskSize` = ?, `Label` = ?"
                     " WHERE `ROWID` = ?")
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

    def query_disks(self, filter: str) -> List[List[str]]:
        disks = []
        for row in self._exec_query(_DISKS_SELECT, (), commit=False):
            if filter and filter not in row[1:3]:
                continue
            disks.append(row)
        return disks

    def list_disks(self, filter: str) -> None:
        headers = ["DiskID", "UUID", "Label", "DiskSize"]
        print_table(self.query_disks(filter), headers)

    def update_disk(self, filter: str) -> None:
        disks = self.query_disks(filter)
        if len(disks) > 1:
            raise ValueError(f"More then one disk is matching UUID {filter}")
        disk_info = get_disk_info(disks[0][1])
        self._exec_query(
            _DISK_UPDATE_SIZE,
            (int(disk_info["fssize"]) // 1024,
             disk_info["label"],
             disks[0][0]),
            commit=True)

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


def update_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.update_disk(args.disk)
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
        "-s", "--size", help="Calculate used space", action="store_true")

    list_dir = subparsers.add_parser(
        "list-dir", help="List directory with statistic")
    list_dir.set_defaults(func=list_dir_command, cmd_name="list-dir")
    list_dir.add_argument("dir_path", type=str, help="Path to dir to list")
    list_disks.add_argument(
        "-r", "--recursive", help="List dir recursively", action="store_true")

    update_disk = subparsers.add_parser(
        "update-disk", help="Update disk with given UUID")
    update_disk.set_defaults(func=update_disk_command, cmd_name="update-disk")

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
