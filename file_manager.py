#!/usr/bin/python3
"""Utils to view/analize file database."""
import argparse
import logging
import re
from pathlib import Path
from shutil import move
from typing import Callable, Dict, List, Tuple

from file_database import DEFAULT_DATABASE, FileManagerDatabase
from file_utils import get_disk_info
from utils import print_table, timestamp2exif_str

_BACKUP_COUNT = ("SELECT `fsrecords`.*, `files`.* FROM ("
                 "  SELECT * FROM ("
                 "    SELECT `FileId`, COUNT(DISTINCT `DiskId`) "
                 "      AS `FileDisksCount` FROM `fsrecords` "
                 "    WHERE `FileId` IS NOT NULL "
                 "    GROUP BY `FileId`) "
                 "  WHERE `FileDisksCount` <= ?) AS `count_files` "
                 "INNER JOIN `fsrecords` ON "
                 "`count_files`.`FileId` = `fsrecords`.`FileId` "
                 "INNER JOIN `files` ON "
                 "`count_files`.`FileId` = `files`.`ROWID` "
                 "WHERE `DiskId` = ? {}")
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
_MOVE_FS_RECORD = ("UPDATE `fsrecords` SET `ParentId` = ?, `Name` = ? "
                   "WHERE `ROWID` = ?")


_SORT_OPTIONS = ["id", "uuid", "label", "disk-size", "files-size", "usage"]
_SORT_OPTIONS_UNIQUE = _SORT_OPTIONS[:]
_SORT_OPTIONS_UNIQUE.insert(5, "unique-size")
_SORT_OPTIONS_UNIQUE += ["unique-usage", "unique-percent"]


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

    def backups_count(
            self, disk: str, count_limit: int, parent_path: str) -> int:
        self.set_disk_by_name(disk)
        extra_query = ""
        params = [count_limit, self._disk_id]
        if parent_path:
            parent_id = self.get_dir_id(
                parent_path.split("/"), insert_dirs=False)
            subdirs = self.query_subdirs(parent_id, recursively=True)
            extra_query = f" AND `ParentId` IN (?{',?' * len(subdirs)})"
            params.append(parent_id)
            params.extend(subdirs)
        files_list = []
        for row in self._exec_query(
                _BACKUP_COUNT.format(extra_query), params, commit=False):
            files_list.append([
                self.get_path(row[1]), row[0], row[7], row[3], row[8]])
        headers = [f"Path on disk {self._disk_label}",
                   "Name", "File Size, B", "File Date", "File SHA1"]
        formats: List[Callable] = [
            str,
            str,
            str,
            timestamp2exif_str,
            str
        ]
        print_table(sorted(files_list, key=lambda info: info[0]),
                    headers, aligns=["<", "<", ">", ">", "<"], formats=formats)
        return 0

    def list_disks(self, filter: str, cal_size: bool, sort_by: str) -> None:
        disks_list = self.query_disks(filter)
        if not disks_list:
            logging.warning(f"Filter {filter} does not match any disk")
            return
        headers = ["DiskID", "UUID", "Label", "DiskSize, MiB"]
        if not cal_size:
            print_table(disks_list, headers)
            return

        headers.extend(["FilesSize, MiB", "Usage, %"])
        id_list = tuple(int(disk[0]) for disk in disks_list)
        disk_usage = self.query_disks("", cal_size=True, id_list=id_list)
        sort_idx = _SORT_OPTIONS.index(sort_by)
        if sort_idx >= len(disk_usage[0]):
            sort_idx = 0
        print_table(sorted(disk_usage, key=lambda info: info[sort_idx]),
                    headers)

    def update_disk(self, filter: str) -> None:
        disks_list = self.query_disks(filter)
        if len(disks_list) > 1:
            raise ValueError(f"More then one disk is matching UUID {filter}")
        disk_info = get_disk_info(disks_list[0][1])
        self._exec_query(
            _DISK_UPDATE_SIZE,
            (int(disk_info.get("fssize", disk_info.get("size", 0))) // 1024,
             disk_info["label"],
             disks_list[0][0]),
            commit=True)

    def unique_files(self, filter: str, sort_by: str) -> None:
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
        sort_idx = _SORT_OPTIONS_UNIQUE.index(sort_by)
        if sort_idx >= len(unique_files[0]):
            sort_idx = 0
        print_table(sorted(unique_files, key=lambda info: info[sort_idx]),
                    headers)

        for row in self._exec_query(_UNIQUE_FILES_SIZE, (), commit=False):
            print(f"Total size of unique files is {row[0]} MiB")

    def list_dir(
                self, disk: str, dir_path: str, recursive: bool,
                summary: bool = False, only_count: bool = False, print_sha: bool = False
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
            dir_size += int(row[5]) if row[5] else 0
            if row[5]:
                files_count += 1
            else:
                subdir_count += 1

        if not (summary or only_count):
            print_dir_content(dir_path, dir_content, print_sha)

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

    def get_unique_files(self, disk: str, dir_ids: list[int], disk_index: int, exclude_path: list[str]) -> Tuple[int, int, int, int]:
        """Generates dictionary self._baseline_file_disks of unique files under provided dir_id
            Returned elements:
            - Number of unique files under dir_ids on current disk
            - Total size of unique files under dir_ids on current disk
            - Number of new unique files added to baseline dictionary, 0 for disk with index 0
            - Total size of unique files added to baseline dictionary, 0 for disk with index 0
        """
        self.set_disk_by_name(disk)
        if not disk_index:
            # Dict of file_id and list of disks that have it
            logging.debug("Inited self._baseline_file_disks")
            self._baseline_file_disks: Dict[str, List[str]] = {}
        logging.debug(
            f"Collecting unique files from disk {self.disk_name} dir_id={dir_ids}, disk_index={disk_index}")
        files_count = 0
        dir_size = 0
        new_files_count = 0
        new_files_size = 0
        subdirs = dir_ids.copy()
        subdirs_count = 0
        baseline_files_count = 0
        baseline_files_size = 0
        while subdirs:
            cur_dir_id = subdirs.pop()
            if any(re.match(exclude_pattern, self.get_path(cur_dir_id)) for exclude_pattern in exclude_path or []):
                logging.debug(f"Skipping dir_id {cur_dir_id} {self.get_path(cur_dir_id)} matched pattern {exclude_path}")
                continue
            for row in self._exec_query(
                    _DIR_LIST_SELECT, (cur_dir_id,), commit=False):
                if not row[5]:  # element is a dir
                    subdirs_count += 1
                    subdirs.append(row[0])
                    continue

                if row[4] in self._baseline_file_disks:
                    if disk not in self._baseline_file_disks[row[4]]:
                        self._baseline_file_disks[row[4]].append(disk)
                        baseline_files_count += 1
                        baseline_files_size += int(row[5])
                    continue

                self._baseline_file_disks[row[4]] = [disk]
                files_count += 1
                dir_size += int(row[5])
                if disk_index:
                    new_files_count += 1
                    new_files_size += int(row[5])
                    logging.debug(f"New file: {self.disk_name}/{self.get_path(cur_dir_id)}/{row[1]}")

        path_list = [self.get_path(dir_id) for dir_id in dir_ids]
        logging.debug(f"Disk {disk} under {','.join(path_list)} have {files_count} unique files, size {dir_size} in {subdirs_count} subdirs, baseline count = {baseline_files_count}, size = {baseline_files_size}")
        logging.debug(f"Size of self._baseline_file_disks {len(self._baseline_file_disks)}")
        if disk_index:
            logging.info(f"Disk {disk} under {','.join(path_list)} have {new_files_count} new unique files, size {new_files_size}")
        return (files_count, dir_size, new_files_count, new_files_size)

    def path_redundancy(self, disks: List[str], paths: List[str], exclude_path: list[str], files_count_limit: int=1) -> None:
        path_id = { disk: [self.get_path_on_disk(disk, path) for path in paths if self.get_path_on_disk(disk, path)] for disk in disks}
        logging.info(f"Calculating redundancy of {','.join(paths)} on disks {','.join(path_id.keys())}")
        logging.debug(path_id)
        disk_index = 0
        disk_status = {}
        for disk, dir_ids in path_id.items():
            disk_status[disk] = self.get_unique_files(disk, dir_ids, disk_index, exclude_path)
            disk_index += 1
        limited_files_id = [file_id for file_id, disk_list in self._baseline_file_disks.items() if len(disk_list) <= files_count_limit]
        for path in paths:
            for disk_label, file_path_list in self.get_file_path_on_disk(limited_files_id, disks, parent_root_path=path, exclude_path=exclude_path).items():
                for file_path in file_path_list:
                    logging.info(f"File {file_path} only present on disk {disk_label}")
        logging.info(f"{len(limited_files_id)} files have less copies then {files_count_limit} on disks {','.join(disks)}")

    def move_fs_item(
            self, disk: str, src: str, dst: str, dry_run: bool) -> int:
        self.set_disk_by_name(disk)
        self.set_mountpoint()

        src_path = self.mountpoint / src
        if not src_path.exists():
            raise ValueError(f"Path {src} does not exist on disk {disk}")
        is_file = src_path.is_file()
        if not is_file:
            if src_path.is_mount():
                raise ValueError(f"Path {src} is mount point for {disk}")
            if not src_path.is_dir():
                raise ValueError(
                        f"Path {src} is neither file not dir on {disk}")

        parent_id = self.get_dir_id(src.split("/")[:-1])
        logging.debug(
            f"Found src parent fsrecord_id {parent_id} for {src_path.parent}")
        object_id = self.get_fsrecord_id(src_path.name, parent_id,
                                         is_file=is_file)
        logging.debug(f"Found src fsrecord_id {object_id} for {src_path}")

        target_parent_from_mount = dst.split("/")[:-1]
        if dst.endswith("/"):
            dst += src_path.name
        dst_path = self.mountpoint / dst
        if dst_path.exists():
            raise ValueError(f"Destination path {dst_path} already exist")
        if not dst_path.parent.exists():
            dst_path.parent.mkdir(parents=True)
        dst_parent_id = self.get_dir_id(
                target_parent_from_mount, insert_dirs=False)
        logging.debug(f"Found dst parent fsrecord_id {dst_parent_id} for "
                      f"{dst_path.parent}")
        logging.debug(f"Moving dir {src_path} to {dst_path}")

        if not dry_run and move(src_path, dst_path) == dst_path:
            self._exec_query(
                    _MOVE_FS_RECORD, (dst_parent_id, dst_path.name, object_id),
                    commit=True)
        elif dry_run:
            logging.info(f"Dry run, DB parent undate {parent_id}->"
                         f"{dst_parent_id}, name {dst_path.name} for fsrecord "
                         f"{object_id} skipped")
        return 0


def print_dir_content(dir_path: str, dir_content: List[List[str]], print_sha: bool) -> None:
    headers = ["Name", "Size", "File Date", "Hash Date"]
    if print_sha:
        headers.append("SHA1")
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
    file_db.list_disks(args.disk, args.size, args.sort)
    return 0


def list_dir_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing directory."""
    try:
        file_db.list_dir(
            args.disk, args.dir_path, args.recursive, summary=args.summary, print_sha=args.print_sha)
        return 0
    except ValueError:
        print(f"Failed to find dir path {args.dir_path} on drive {args.disk}")
        return 1


def move_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.move_fs_item(
            args.disk, args.src_path, args.dst_path, args.dry_run)


def backups_count_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.backups_count(args.disk, args.count_limit, args.parent_path)


def update_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.update_disk(args.disk)
    return 0


def unique_files_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.unique_files(args.disk, args.sort)
    return 0


def path_redundancy_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.path_redundancy(args.disks, args.path, args.exclude_path, files_count_limit = args.count_limit)
    return 0

def delete_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.delete_disk(args.disk)

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
    disk_required = ["backups-count", "list-dir", "move", "update-disk", "delete-disk"]
    arg_parser.add_argument(
        "-d", "--disk", type=str,
        help=("Disk label or UUID to process, requirted for " +
              ", ".join(disk_required)))
    subparsers = arg_parser.add_subparsers(
        help="Commands supported by CLI tool", dest="command")

    backups_count = subparsers.add_parser(
        "backups-count", help=("Displays files of the disk(s) with specific "
                               "number of copies"))
    backups_count.set_defaults(func=backups_count_command,
                               cmd_name="backups-count")
    backups_count.add_argument(
        "-c", "--count-limit",
        help="Max number of file's backups to select, default 1",
        type=int, default=1)
    backups_count.add_argument("-p", "--parent-path", type=str,
                               help="Filter files by parent path")

    list_disks = subparsers.add_parser(
        "list-disks", help="List disks with statistic")
    list_disks.set_defaults(func=list_disks_command, cmd_name="list-disks")
    list_disks.add_argument(
        "-s", "--size", help="Calculate space used by files",
        action="store_true")
    list_disks.add_argument(
        "--sort", help="Sort output", choices=_SORT_OPTIONS,
        default=_SORT_OPTIONS[0])

    list_dir = subparsers.add_parser(
        "list-dir", help="List directory with statistic")
    list_dir.set_defaults(func=list_dir_command, cmd_name="list-dir")
    list_dir.add_argument("dir_path", type=str, help="Path to dir to list")
    list_dir.add_argument(
        "-r", "--recursive", help="List dir recursively", action="store_true")
    output_format = list_dir.add_mutually_exclusive_group()
    output_format.add_argument(
        "-s", "--summary", help="Print only summary", action="store_true")
    output_format.add_argument(
        "-p", "--print-sha", help="Print SHA for each file", action="store_true")

    move_object = subparsers.add_parser(
        "move", help="Move dir, update DB accordingly")
    move_object.set_defaults(func=move_command, cmd_name="move")
    move_object.add_argument("src_path", type=str, help="Path to dir to move")
    move_object.add_argument("dst_path", type=str, help="Destination path")
    move_object.add_argument("--dry-run",
                             help="Do not move dir, run all checks",
                             action="store_true")

    update_disk = subparsers.add_parser(
        "update-disk", help="Update disk with given UUID")
    update_disk.set_defaults(func=update_disk_command, cmd_name="update-disk")

    unique_files = subparsers.add_parser(
        "unique-files", help="Calculate size of unique files")
    unique_files.set_defaults(
        func=unique_files_command, cmd_name="unique-files")
    unique_files.add_argument(
        "--sort", help="Sort output", choices=_SORT_OPTIONS_UNIQUE,
        default=_SORT_OPTIONS_UNIQUE[0])

    path_redundancy = subparsers.add_parser(
        "path-redundancy", help="Calculate redundancy of specific path")
    path_redundancy.set_defaults(func=path_redundancy_command, cmd_name="path-redundancy")
    path_redundancy.add_argument(
        "--disks", help="Disks to include, at least 2 disks required", type=str, nargs="+", required=True)
    path_redundancy.add_argument(
        "--path", help="Directory path to include", type=str, nargs='*', required=True)
    path_redundancy.add_argument(
        "-e", "--exclude-path", type=str, nargs='*', help="List of path to exclude")
    path_redundancy.add_argument(
        "-c", "--count-limit", type=int, default=1, help="Max number of file's backups to select, default 1")

    delete_disk = subparsers.add_parser(
        "delete-disk", help="Delete disk and file records on it")
    delete_disk.set_defaults(func=delete_disk_command, cmd_name="delete-disk")

    args = arg_parser.parse_args()
    if args.cmd_name in disk_required and not args.disk:
        arg_parser.error(f"-d DISK argument is required for {args.cmd_name}")

    if args.cmd_name == "path-redundancy" and len(args.disks) < 2:
        arg_parser.error(f"--disks DISK1,DISK2,... argument is require at least 2 disks for {args.cmd_name}")

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
