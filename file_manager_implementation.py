import logging
import re
from pathlib import Path
from shutil import move
from typing import Callable, Dict, List, Tuple

from file_database import FileManagerDatabase
from file_utils import get_disk_info
from utils import print_table, timestamp2exif_str

SORT_OPTIONS = ["id", "uuid", "label", "disk-size", "files-size", "usage"]
SORT_OPTIONS_UNIQUE = SORT_OPTIONS[:]
SORT_OPTIONS_UNIQUE.insert(5, "unique-size")
SORT_OPTIONS_UNIQUE += ["unique-usage", "unique-percent"]

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
_FILD_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`Name`, "
                "`fsrecords`.`FileDate`, `fsrecords`.`SHA1ReadDate`, "
                "`files`.`ROWID`, `files`.`FileSize`, `SHA1`, `ParentId`, `DiskId`"
                " FROM `fsrecords` LEFT JOIN `files`"
                " ON `files`.`ROWID` = `fsrecords`.`FileId`"
                " WHERE (`Name` LIKE ? OR `CanonicalName` LIKE ?) AND `fsrecords`.`FileId` IS {file}NULL"
                " ORDER BY `Name`")


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
        sort_idx = SORT_OPTIONS.index(sort_by)
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
        sort_idx = SORT_OPTIONS_UNIQUE.index(sort_by)
        if sort_idx >= len(unique_files[0]):
            sort_idx = 0
        print_table(sorted(unique_files, key=lambda info: info[sort_idx]),
                    headers)

        for row in self._exec_query(_UNIQUE_FILES_SIZE, (), commit=False):
            print(f"Total size of unique files is {row[0]} MiB")

    def _get_dir_content(self, dir_id: int, sort_index: int = -1) -> tuple[list[tuple[int, str, float, float, int, int, str]], int, int, int]:
        """Returns list dir elements as first element, size of files in dir, count of files and subdirs as a tuple:
            0: `fsrecords`.`ROWID`
            1: `fsrecords`.`Name`
            2: `fsrecords`.`FileDate`
            3: `fsrecords`.`SHA1ReadDate`
            4: `files`.`ROWID`
            5: `files`.`FileSize`
            6: `SHA1`
            dir_content list sorted by sort_index and name if possible
        """
        dir_content = []
        dir_size = 0
        files_count = 0
        subdir_count = 0
        for row in self._exec_query(
                _DIR_LIST_SELECT, (dir_id,), commit=False):
            dir_content.append(row)
            dir_size += int(row[5]) if row[5] else 0
            if row[4]:
                files_count += 1
            else:
                subdir_count += 1
        if dir_content and sort_index != -1 and sort_index < len(dir_content[0]):
            dir_content.sort(key=lambda x: x[sort_index] or f" {x[1]}")
        return (dir_content, dir_size, files_count, subdir_count)

    def list_dir(
                self, disk: str, dir_path: str, recursive: bool,
                summary: bool = False, only_count: bool = False, print_sha: bool = False
            ) -> Tuple[int, int, int]:
        self.set_disk_by_name(disk)
        self._cur_dir_id = self.get_dir_id(
            dir_path.split("/"), insert_dirs=False)
        logging.debug(
            f"Listing dir {self.disk_name}/{dir_path} id={self._cur_dir_id}")

        dir_content, dir_size, files_count, subdir_count = self._get_dir_content(self._cur_dir_id, sort_index=1)

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

    def get_disk_dir_id(self, disk_path: str) -> tuple[int, int, str]:
        disk_parts = disk_path.split("/")
        disk_rows = {row[0]: row for row in self._query_disks([disk_parts[0]])}
        if not disk_rows:
            raise ValueError(f"Failed to find disk {disk_parts[0]} in database")
        if len(disk_rows) > 1:
            raise ValueError(f"Disk param {disk_parts[0]} returns more than one disk with UUIDs {','.join([row[1] for row in disk_rows.values()])} from database")
        for disk_id, row in disk_rows.items():
            self._set_disk(int(disk_id), row[1], int(row[2]), row[3])
            self.set_top_dir()
            self._cur_dir_id = self.get_dir_id(disk_parts[1:], insert_dirs=False)
        return (self._disk_id, self._cur_dir_id, disk_parts[0])

    def _get_nonempty_dir_row(self, row: tuple[int, str, float, float, int, int, str]) -> list[tuple[int, str, float, float, int, int, str]]:
        if row[4]:
            return []
        _, _, files_count, subdir_count = self._get_dir_content(int(row[0]))
        if files_count + subdir_count:  # ignore empty dirs
            return [row]
        return []

    def diff_dirs(self, disk1_name: str, dir1_id: int, disk2_name: str, dir2_id: int) -> int:
        """Recursively compares dirs. Returns 0 is matching, > 0 otherwise"""
        result = 0
        dir1_content, _, files1_count, subdir1_count = self._get_dir_content(dir1_id, sort_index=6)
        dir2_content, _, files2_count, subdir2_count = self._get_dir_content(dir2_id, sort_index=6)
        idx_1 = 0
        idx_2 = 0
        disk1_path = f"{disk1_name}/{self.get_path(dir1_id)}"
        disk2_path = f"{disk2_name}/{self.get_path(dir2_id)}"
        missing_in_dir1_files = []
        missing_in_dir2_files = []
        missing_in_dir1_subdirs = []
        missing_in_dir2_subdirs = []
        while idx_1 < files1_count + subdir1_count and idx_2 < files2_count + subdir2_count:
            row1 = dir1_content[idx_1]
            row2 = dir2_content[idx_2]
            if not row1[6] and not row2[6]:  # both are dirs
                if row1[1] == row2[1]:
                    result += self.diff_dirs(disk1_name, int(row1[0]), disk2_name, int(row2[0]))
                    idx_1 += 1
                    idx_2 += 1
                elif row1[1] < row2[1]:
                    missing_in_dir2_subdirs.extend(self._get_nonempty_dir_row(row1))
                    idx_1 += 1
                else:
                    missing_in_dir1_subdirs.extend(self._get_nonempty_dir_row(row2))
                    idx_2 += 1
            elif not row1[6]:   # subdir only in dir1
                missing_in_dir2_subdirs.extend(self._get_nonempty_dir_row(row1))
                idx_1 += 1
            elif not row2[6]:   # subdir only in dir2
                missing_in_dir1_subdirs.extend(self._get_nonempty_dir_row(row2))
                idx_2 += 1
            elif row1[6] == row2[6]:   # referring same file from both dirs
                idx_1 += 1
                idx_2 += 1
            elif row1[6] < row2[6]:
                missing_in_dir2_files.append(row1)
                idx_1 += 1
            else:
                missing_in_dir1_files.append(row2)
                idx_2 += 1
            # Skip identical files to avoid diff on already compared files
            while idx_1 and idx_1 < files1_count + subdir1_count and row1[6] and row1[0] != dir1_content[idx_1][0] and dir1_content[idx_1][6] == row1[6]:
                idx_1 += 1
            while idx_2 and idx_2 < files2_count + subdir2_count and row2[6] and row2[0] != dir2_content[idx_2][0] and dir2_content[idx_2][6] == row2[6]:
                idx_2 += 1

        while idx_1 < files1_count + subdir1_count:
            row1 = dir1_content[idx_1]
            if row1[6]:
                missing_in_dir2_files.append(row1)
            else:
                missing_in_dir2_subdirs.extend(self._get_nonempty_dir_row(row1))
            idx_1 += 1

        while idx_2 < files2_count + subdir2_count:
            row2 = dir2_content[idx_2]
            if row2[6]:
                missing_in_dir1_files.append(row2)
            else:
                missing_in_dir1_subdirs.extend(self._get_nonempty_dir_row(row2))
            idx_2 += 1

        missing_in_dir1_files.sort(key=lambda x: x[1])
        missing_in_dir2_files.sort(key=lambda x: x[1])
        missing_in_dir1_subdirs.sort(key=lambda x: x[1])
        missing_in_dir2_subdirs.sort(key=lambda x: x[1])
        for row2 in missing_in_dir1_files:
            print(f"file {row2[1]} SHA1 {row2[6]} present in {disk2_path} and missing in {disk1_path}")
        for row1 in missing_in_dir2_files:
            print(f"file {row1[1]} SHA1 {row1[6]} present in {disk1_path} and missing in {disk2_path}")
        for row2 in missing_in_dir1_subdirs:
            print(f"subdir {row2[1]} present in {disk2_path} and missing in {disk1_path}")
        for row1 in missing_in_dir2_subdirs:
            print(f"subdir {row1[1]} present in {disk1_path} and missing in {disk2_path}")

        return result + len(missing_in_dir1_files) + len(missing_in_dir2_files) + len(missing_in_dir1_subdirs) + len(missing_in_dir2_subdirs)

    def diff(self, disk1_path: str, disk2_path: str) -> int:
        _, dir2_id, disk2_name = self.get_disk_dir_id(disk2_path)
        _, dir1_id, disk1_name = self.get_disk_dir_id(disk1_path)
        return self.diff_dirs(disk1_name, dir1_id, disk2_name, dir2_id)

    def find(self, disk: str, dir: bool, name: str, include_path: list[str], exclude_path: list[str], size: str, print_sha: bool) -> int:
        params = (name.replace("?", "_").replace("*", "%"),)
        matching_list = []
        for row in self._exec_query(
                _FILD_SELECT.format(file="" if dir else "NOT "), params, commit=False):
            path = self.get_path(row[7])
            if include_path and not any(re.match(include_pattern.replace("?", ".").replace("*", ".*"), path) for include_pattern in include_path):
                continue
            if exclude_path and any(re.match(exclude_pattern.replace("?", ".").replace("*", ".*"), path) for exclude_pattern in exclude_path):
                continue
            row[8] = path
            matching_list.append(row)


        print_find_results(matching_list, print_sha)
        return 0

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


def print_dir_content(dir_path: str, dir_content: list[tuple[int, str, float, float, int, int, str]], print_sha: bool) -> None:
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


def print_find_results(find_results: list[tuple[int, str, float, float, int, int, str, int, str]], print_sha: bool) -> None:
    headers = ["Name", "Path", "Size", "File Date", "Hash Date"]
    if print_sha:
        headers.append("SHA1")
    indexes = [1, 8, 5, 2, 3, 6]
    formats: List[Callable] = [
        str,
        str,
        lambda x: str(x) if x else "dir",
        timestamp2exif_str,
        timestamp2exif_str,
        str
    ]
    aligns = ["<", "<", ">", ">", ">", "<"]
    print_table(
        find_results, headers, indexes=indexes,
        formats=formats, aligns=aligns)
