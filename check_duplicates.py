#!/usr/bin/python3
"""Search duplicate files in database."""

import argparse
import logging
import sys
from pathlib import Path

from file_database import DISK_SELECT, FileManagerDatabase

DEFAULT_DATABASE = Path("/var/lib/file-manager/fileManager.db")

DISK_SELECT_LABEL = ("SELECT `ROWID`, `UUID`, `DiskSize`, `Label`"
                     " FROM `disks` WHERE Label = ?")


class FileDuplicates(FileManagerDatabase):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path):
        super().__init__(db_path, 0)

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

    def search_duplicate_folders(self, max_diff: int) -> None:
        pass


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
        "--max-diff",
        type=int,
        help="Number of different files to consider as duplicate.",
        default=0)
    arg_parser.add_argument("-v", "--verbose",
                            help="Print verbose output",
                            action="count", default=0)
    args = arg_parser.parse_args(argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    with FileDuplicates(args.database) as file_db:
        file_db.set_disk(args.uuid, 0, args.label)
        file_db.search_duplicate_folders(args.max_diff)


if __name__ == "__main__":
    main(sys.argv)
