"""File database module."""

import logging
import re
from pathlib import Path
from time import time
from typing import Dict, List, Optional, Tuple

import file_utils
from db_utils import SQLite3connection
from utils import timestamp2exif_str

DEFAULT_DATABASE = Path("/var/lib/file-manager/fileManager.db")
_DISK_SIZE_DIFF_LIMIT = 0.005

_DISK_SELECT = ("SELECT `ROWID`, `UUID`, `DiskSize`, `Label`"
                " FROM `disks` WHERE `UUID` IN (?{}) OR `Label` IN (?{})")
_DISK_INSERT = ("INSERT INTO `disks` (`UUID`, `DiskSize`, `Label`) "
                "VALUES (?, ?, ?)")

_TOP_DIR_SELECT = ("SELECT `ROWID` FROM `fsrecords` WHERE `DiskId` = ? AND"
                   " `ParentId` IS NULL AND `Name` = '' AND `FileId` IS NULL")
_FSRECORD_SELECT = ("SELECT `ROWID`, `SHA1ReadDate` FROM `fsrecords` WHERE"
                    " `DiskId` = ? AND `ParentId` = ? AND `Name` = ? AND"
                    " `FileId` IS {} NULL")
_DIR_PARENT_SELECT = ("SELECT `ParentId`, `Name` FROM `fsrecords` "
                      "WHERE `ROWID` = ?")
_FSDIR_INSERT = ("INSERT INTO `fsrecords` (`Name`, `ParentId`, `DiskId`) "
                 "VALUES (?, ?, ?)")
_FSFILE_INSERT = ("INSERT INTO `fsrecords` (`Name`, `ParentId`, `DiskId`, "
                  "`FileDate`, `FileId`, `SHA1ReadDate`) "
                  "VALUES (?, ?, ?, ?, ?, ?)")
_FSFILE_UPDATE = ("UPDATE `fsrecords` SET `FileDate` = ?, `FileId` = ?, "
                  "`SHA1ReadDate` = ? WHERE `ROWID` = ?")
_DIR_CLEAN = ("DELETE FROM `fsrecords` WHERE `DiskId` = ? AND"
              " `ParentId` = ? AND `FileId` IS {} NULL")
_DIR_CLEAN_NAMES = " AND `Name` NOT IN (?"
_FSFILE_DELETE = ("DELETE FROM `fsrecords` WHERE `ROWID` IN (?")
_FS_ORFANS_SELECT = ("SELECT `child`.`ROWID`, `child`.`Name`, `parent`.`Name` "
                     "FROM `fsrecords` AS `child` "
                     "LEFT JOIN `fsrecords` AS `parent` "
                     "ON `parent`.ROWID = `child`.`ParentId` "
                     "WHERE `child`.`ParentId` IS NOT NULL "
                     "ORDER BY `child`.`ROWID`")

_FILE_SELECT = ("SELECT `ROWID`, `EarliestDate`, `CanonicalName`, "
                "`CanonicalType`, `MediaType` FROM `files` WHERE `SHA1` = ?")
_FILE_TIME_UPDATE = ("UPDATE `files` SET `EarliestDate` = ? WHERE "
                     "`EarliestDate` > ? AND `ROWID` = ? ")
_FILE_INSERT = ("INSERT INTO `files` (`FileSize`, `SHA1`, `EarliestDate`, "
                "`CanonicalName`, `CanonicalType`, `MediaType`) "
                "VALUES (?, ?, ?, ?, ?, ?)")
_FILE_ORFANS_SELECT = ("SELECT `files`.`ROWID`,`CanonicalName`, "
                       "`CanonicalType`, `FileSize`, `SHA1`, `EarliestDate`,"
                       "COUNT(DISTINCT `fsrecords`.`ROWID`) as `ref_count` "
                       "FROM `files` "
                       "LEFT JOIN `fsrecords` "
                       "ON `files`.`ROWID` = `fsrecords`.`FileId` "
                       "GROUP BY `files`.`ROWID`, `CanonicalName`, "
                       "`CanonicalType`, `EarliestDate`, `SHA1`, `FileSize`"
                       "ORDER BY `ref_count`, `files`.`ROWID`")
_FILE_DELETE = ("DELETE FROM `files` WHERE `ROWID` IN (?")

_FS_FILE_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`SHA1ReadDate`, "
                   "`fsrecords`.`FileDate`, `files`.`ROWID`, "
                   "`files`.`FileSize`, `SHA1`"
                   " FROM `fsrecords` INNER JOIN `files`"
                   " ON `files`.`ROWID` = `fsrecords`.`FileId`"
                   " WHERE `DiskId` = ? AND `ParentId` = ? AND `Name` = ?"
                   " AND `FileId` IS NOT NULL")
_FILE_ON_DISK_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`SHA1ReadDate`, "
                        "`fsrecords`.`FileDate`, `ParentId`, "
                        "`files`.`FileSize`, `SHA1`"
                        "FROM `fsrecords` INNER JOIN `files` "
                        "ON `files`.`ROWID` = `fsrecords`.`FileId` "
                        "WHERE `DiskId` = ? AND `FileId` IN(?{})")
_SELECT_SUBDIRS = ("SELECT `ROWID`, `Name`, `ParentId` FROM `fsrecords` "
                   "WHERE `ParentId` IN(?{}) AND `FileId` IS NULL "
                   "ORDER BY `ParentId`, `Name`")

_DELETE_DISK_FILES = "DELETE FROM `fsrecords` WHERE `DiskId` = ?"
_DELETE_DISK = "DELETE FROM `disks` WHERE `UUID` = ? AND `ROWID` = ?"


class FileManagerDatabase(SQLite3connection):
    """Representation of fileManager database and relevant queries."""

    def __init__(
            self, db_path: Path, rehash_time: float):
        super().__init__(db_path)
        self._rehash_time = rehash_time
        # Details on current disk
        self._disk_id: int = 0
        self._disk_uuid: Optional[str] = None
        self._disk_size: Optional[int] = None
        self._disk_label: Optional[str] = None
        # Ref to top dir on disk of the current dir
        self._top_dir_id: int = 0
        self._id_cache: Dict[int, Dict[str, int]] = {}
        self._path_cache: Dict[int, str] = {}
        self._cur_dir_id: int = 0
        self._cur_dir_path: Path = Path(".")
        self.mountpoint: Path = Path(".")

    def _set_disk(self, id: int, uuid: str, size: int, label: str) -> None:
        self._disk_id = id
        self._disk_uuid = uuid
        self._disk_size = size
        self._disk_label = label
        if self._disk_id not in self._id_cache:
            self._id_cache[self._disk_id] = {}

    @property
    def disk_name(self) -> str:
        return self._disk_label or self._disk_uuid or ""

    def set_mountpoint(self):
        """Request lsblk disk info, raises ValueError if not mounted."""
        self.mountpoint = Path(
                file_utils.get_disk_info(self._disk_uuid)["mountpoint"])

    def _save_path_cache(self, path_id: int, path: str) -> None:
        """Updates id and path caches."""
        self._id_cache[self._disk_id][path] = path_id
        self._path_cache[path_id] = path

    def handle_file_orfans(self, clear_orfan_files: bool = False) -> int:
        """Searching for orfans in files."""
        orfans_count = 0
        orfans_list = []
        for row in self._exec_query(_FILE_ORFANS_SELECT, (), commit=False):
            if not row[6]:
                logging.warning(
                    f"Found orpfan file {row[1]}.{row[2]} size {row[3]}, SHA1 "
                    f"{row[4]}, earliest date {timestamp2exif_str(row[5])}")
                if clear_orfan_files:
                    orfans_list.append(row[0])
                orfans_count += 1
        if orfans_list:
            self._exec_query(
                _FILE_DELETE + ", ?" * (len(orfans_list) - 1) + ")",
                tuple(orfans_list), commit=True)
        return orfans_count

    def remove_fsrecords_orfans(self) -> int:
        """Removes one level of fsrecords orfans."""
        orfans_count = 0
        orfans_list = []
        for row in self._exec_query(_FS_ORFANS_SELECT, (), commit=False):
            if row[2] is None:
                logging.warning(f"Found fsrecors file {row} to be deleted")
                orfans_list.append(row[0])
                orfans_count += 1
        if orfans_list:
            logging.warning(f"remove_fsrecords_orfans: {orfans_list}")
            self._exec_query(
                _FSFILE_DELETE + ", ?" * (len(orfans_list) - 1) + ")",
                tuple(orfans_list), commit=True)
        return orfans_count

    def _query_disks(self, names: List[str]) -> List[List[str]]:
        return  self._exec_query(_DISK_SELECT.format(",?" * (len(names) - 1), ",?" * (len(names) - 1)), (*names, *names), commit=False)

    def set_disk(self, uuid: str, size: int, label: str) -> None:
        """Create/update disk details in DB."""
        for row in self._query_disks([uuid, uuid]):
            self._set_disk(int(row[0]), row[1], int(size), label)
            # TODO: support free disk space tracking in DB
            if abs(float(row[2]) / float(size) - 1) > _DISK_SIZE_DIFF_LIMIT or row[3] != label:
                logging.error("Size or label of disk UUID %s changed: "
                              "%d -> %d, %s -> %s",
                              uuid, row[2], size, row[3], label)
                raise ValueError(f"Disk UUID {uuid} details changed: "
                                 f"{row[2]}->{size}, {row[3]}->{label}")
            self.set_top_dir()
            break
        else:
            self._exec_query(_DISK_INSERT, (uuid, size, label))
            self.set_disk(uuid, size, label)

    def set_disk_by_name(self, name: str) -> None:
        for row in self._query_disks([name]):
            self._set_disk(int(row[0]), row[1], int(row[2]), row[3])
            self.set_top_dir()
            break
        else:
            raise ValueError(
                f"DB does not have info on disk with UUID={name} "
                f"or label={name}")
        logging.debug(
            f"Processing disk id={self._disk_id}, size={self._disk_size}, "
            f"label={self._disk_label}, UUID={self._disk_uuid}")

    def set_top_dir(self):
        """Loading from DB or creating if missing top dir id."""
        if not self._disk_id:
            raise ValueError("Missing _disk_id")
        for row in self._exec_query(
                _TOP_DIR_SELECT, (self._disk_id,), commit=False):
            self._top_dir_id = row[0]
            self._id_cache[self._disk_id] = {'': self._top_dir_id}
            break
        else:
            self._exec_query(_FSDIR_INSERT, ("", None, self._disk_id))
            self.set_top_dir()

    def get_media_type(self, file_type):
        """Returns media type from file type (extension)."""
        del file_type
        return None

    def get_fsrecord_id(
                self, fsrecord_name, parent_id, is_file=False, insert_dirs=True
                ):
        """Getting dir id for given name/parent, generating if missing."""
        if not self._disk_id:
            raise ValueError("Missing _disk_id")
        for row in self._exec_query(
                _FSRECORD_SELECT.format("NOT" if is_file else ""),
                (self._disk_id, parent_id, fsrecord_name),
                commit=False):
            logging.debug("get_fsrecord_id: %s, parent %r = %r, is_file=%r",
                          fsrecord_name, parent_id, row[0], is_file)
            return row[0]

        if is_file:
            raise ValueError("Missing fsrecord_id for a file")
        if not insert_dirs:
            raise ValueError(f"Missing fsrecord_id for a dir {fsrecord_name} "
                             f"under {parent_id}")

        self._exec_query(
            _FSDIR_INSERT, (fsrecord_name, parent_id, self._disk_id))
        return self.get_fsrecord_id(fsrecord_name, parent_id, is_file=is_file)

    def get_dir_id(
            self, from_mount_path: List[str], insert_dirs: bool = True) -> int:
        """Returns id of the dir from a path."""
        if not from_mount_path:
            return self._top_dir_id
        cached_id = self._id_cache[self._disk_id].get(
                "/".join(from_mount_path))
        if cached_id:
            return cached_id

        cur_path_id = self._top_dir_id
        cur_path_list = []
        for dir_name in from_mount_path:
            cur_path_list.append(dir_name)
            cur_path = "/".join(cur_path_list)
            if cur_path in self._id_cache[self._disk_id]:
                logging.debug(
                        f"Found {cur_path} in _id_cache[{self._disk_id}]")
                cur_path_id = self._id_cache[self._disk_id][cur_path]
            else:
                cur_path_id = self.get_fsrecord_id(
                    dir_name, cur_path_id, insert_dirs=insert_dirs)
                self._save_path_cache(cur_path_id, cur_path)
        return cur_path_id

    def get_path(self, fsrecord_id: int) -> str:
        """Returns path of given fsrecord_id."""
        cached_path = self._path_cache.get(fsrecord_id)
        if cached_path:
            return cached_path
        for row in self._exec_query(
                _DIR_PARENT_SELECT, (fsrecord_id,), commit=False):
            if row[0] == fsrecord_id:
                raise ValueError(f"Parent query returned parent {row[0]} for "
                                 f"{fsrecord_id}")
            if row[0] is None or not row[1]:
                fsrecord_path = ""
                break
            parent_path = self.get_path(row[0])
            if parent_path:
                fsrecord_path = f"{parent_path}/{row[1]}"
            else:
                fsrecord_path = row[1]
            break
        else:
            raise ValueError(f"Failed to find path for {fsrecord_id}")

        self._save_path_cache(fsrecord_id, fsrecord_path)
        return fsrecord_path

    def query_files_on_disk(self, disk_id: int, file_id: List[str]) -> list[str]:
        result = []
        shards_count = 1 + (len(file_id) + 1) // self.SQLITE_LIMIT_VARIABLE_NUMBER
        shard_size = len(file_id) // shards_count
        logging.debug(f"query_files_on_disk: SQLITE_LIMIT_VARIABLE_NUMBER={self.SQLITE_LIMIT_VARIABLE_NUMBER}, len(file_id)={len(file_id)}, shards_count={shards_count}, shard_size={shard_size}")
        for i in range(shards_count):
            for row in self._exec_query(_FILE_ON_DISK_SELECT.format(",?" * (shard_size - 1)), (disk_id, *file_id[i*shard_size:(i+1)*shard_size]), commit=False):
                result.append(self.get_path(row[0]))
            logging.debug(f"query_files_on_disk: got {len(result)} records after shard {i}")
        return result

    def get_file_path_on_disk(self, file_id: List[str], disks: List[str], parent_root_path: Optional[str]= None, exclude_path: Optional[list[str]]= None) -> Dict[str, List[str]]:
        """Return path on disk of specific file_id."""
        if not file_id:
            return {}
        disk_labels = {row[0]: row[3] for row in self._query_disks(disks)}
        path_on_disk: Dict[str, List[str]] = {disk_label: [] for disk_label in disk_labels.values()}
        for disk_id, disk_label in disk_labels.items():
            all_path_on_disk = self.query_files_on_disk(int(disk_id), file_id)
            path_on_disk[disk_label].extend([path for path in all_path_on_disk if (not parent_root_path or path.startswith(f"{parent_root_path}/")) and not any(re.match(exclude_pattern, path) for exclude_pattern in exclude_path or [])])
        return path_on_disk

    def query_subdirs(self, dir: int, recursively: bool = False) -> List[int]:
        """Queries files of given pair of dirs."""
        subdirs = []
        parents = [dir]
        while parents:
            next_parents = []
            for row in self._exec_query(
                    _SELECT_SUBDIRS.format(",?" * (len(parents) - 1)),
                    parents, commit=False):
                subdirs.append(row[0])
                if recursively:
                    next_parents.append(row[0])
                self._save_path_cache(
                    row[0], f"{self.get_path(row[2])}/{row[1]}")
            parents = next_parents
        return subdirs

    def set_cur_dir(self, dir_path: Path):
        """Saving/updating dir with a path to disk root."""
        if not self._top_dir_id:
            raise ValueError("Missing _top_dir_id")
        self._cur_dir_id = self.get_dir_id(
            file_utils.get_path_from_mount(dir_path))
        self._cur_dir_path = dir_path
        logging.debug(f"set_cur_dir: {self._cur_dir_path}={self._cur_dir_id}")

    def clean_cur_dir(self, names, is_files):
        """Deleting from DB files/subdirs missing in names list."""
        if not self._cur_dir_id:
            raise ValueError("Missing _cur_dir_id")
        clean_sql = _DIR_CLEAN.format("NOT" if is_files else "")
        if names:
            clean_sql += _DIR_CLEAN_NAMES + ", ?" * (len(names) - 1) + ")"
        self._exec_query(
            clean_sql, tuple([self._disk_id, self._cur_dir_id] + names))

    def get_db_file_info(self, file_name: str) -> Tuple[
            int, float, float, int, int, str]:
        """Query if exists fsrecords/files details on file.
            Returned fields:
                `fsrecords`.`ROWID`
                `fsrecords`.`SHA1ReadDate`
                `fsrecords`.`FileDate`
                `files`.`ROWID`
                `files`.`FileSize`
                `files`.`SHA1`
        """
        for row in self._exec_query(
                _FS_FILE_SELECT, (self._disk_id, self._cur_dir_id, file_name),
                commit=False):
            return row[0], row[1], row[2], row[3], row[4], row[5]
        return 0, 0, 0, 0, 0, ""

    def select_file_id(self, sha1: str, mtime: float) -> int:
        """Selects file_id (files.ROWID) by SHA, updates mtime if needed."""
        for row in self._exec_query(_FILE_SELECT, (sha1,), commit=False):
            if row[1] > mtime:
                self._exec_query(_FILE_TIME_UPDATE, (mtime, mtime, row[0]))
            return row[0]
        return 0

    def select_update_file_record(
            self, sha1: str, mtime: float, size: int,
            file_name: str, file_type: str) -> int:
        """Select and update or insert file record."""
        if not sha1:
            raise ValueError(f"Empty SHA1 for file {file_name}")
        if not file_name:
            raise ValueError(f"Empty file_name for file with SHA1 {sha1}")
        file_id = self.select_file_id(sha1, mtime)
        if file_id:
            return file_id

        self._exec_query(_FILE_INSERT, (
            size, sha1, mtime, file_name, file_type,
            self.get_media_type(file_type)))
        return self.select_file_id(sha1, mtime)

    def update_fsrecord(
            self, fsrecord_id: int, file_name: str,
            mtime: float, file_id: int) -> None:
        """Update or insert fsrecords."""
        if fsrecord_id:
            self._exec_query(_FSFILE_UPDATE, (
                mtime, file_id, time(), fsrecord_id))
        else:
            self._exec_query(
                _FSFILE_INSERT,
                (file_name, self._cur_dir_id, self._disk_id, mtime, file_id,
                 time()))

    def get_path_on_disk(self, disk: str, path: str) -> int:
        """Returns dir_id of the path on disk."""
        self.set_disk_by_name(disk)
        try:
            dir_id = self.get_dir_id(path.split('/'), insert_dirs=False)
            logging.debug("Found dir {path} with ID {dir_id} on disk {disk}")
            return dir_id
        except ValueError:
            return 0

    def delete_disk(self, disk: str) -> int:
        disk_ids = {row[0]: row[1] for row in self._query_disks([disk])}
        if not disk_ids:
            logging.error(f"Failed to find disk {disk} in database")
            return 1
        if len(disk_ids) > 1:
            logging.error(f"Disk param {disk} returns more than one disk with UUIDs {','.join(disk_ids.values())} from database")
            return 2
        for disk_id, disk_uuid in disk_ids.items():
            if not file_utils.get_confirmation(f"Please confirm delete from DB disk {disk} with UUID {disk_uuid}. Type 'delete': ", ['delete']):
                logging.warning(f"Did not get confirmation for disk {disk} deletion, exiting")
                return 3
            logging.info(f"Deleting file records associated with disk {disk}, UUID {disk_uuid}, DiskId {disk_id}...")
            self._exec_query(_DELETE_DISK_FILES, (disk_id, ))
            logging.info(f"Deleting disk {disk}, UUID {disk_uuid}, DiskId {disk_id}...")
            self._exec_query(_DELETE_DISK, (disk_uuid, disk_id))
            logging.info(f"Disk {disk}, UUID {disk_uuid}, DiskId {disk_id} was deleted from DB with all file records")

        return 0
