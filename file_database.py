"""File database module."""

import logging
from pathlib import Path
from time import time
from typing import Dict, List, Optional, Tuple

import file_utils
from db_utils import SQLite3connection, get_full_path
from utils import timestamp2exif_str

DISK_SELECT = ("SELECT `ROWID`, `UUID`, `DiskSize`, `Label`"
               " FROM `disks` WHERE UUID = ?")
_DISK_UPDATE_SIZE = ("UPDATE `disks` SET `DiskSize` = ?, `Label` = ?"
                     " WHERE `ROWID` = ?")
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

    def _set_disk(self, id: int, uuid: str, size: int, label: str) -> None:
        self._disk_id = id
        self._disk_uuid = uuid
        self._disk_size = size
        self._disk_label = label

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

    def set_disk(self, uuid: str, size: int, label: str) -> None:
        """Create/update disk details in DB."""
        for row in self._exec_query(DISK_SELECT, (uuid,), commit=False):
            self._set_disk(row[0], row[1], size, label)
            # TODO: support free disk space tracking in DB
            if row[2] != size or row[3] != label:
                logging.warning("Size or label of disk UUID %s changed: "
                                "%d -> %d, %s -> %s",
                                uuid, row[2], size, row[3], label)
                self._exec_query(_DISK_UPDATE_SIZE, (size, label, uuid))
            self.set_top_dir()
            break
        else:
            self._exec_query(_DISK_INSERT, (uuid, size, label))
            self.set_disk(uuid, size, label)

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

    def get_fsrecord_id(self, fsrecord_name, parent_id, is_file=False):
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

        self._exec_query(
            _FSDIR_INSERT, (fsrecord_name, parent_id, self._disk_id))
        return self.get_fsrecord_id(fsrecord_name, parent_id, is_file=is_file)

    def get_dir_id(self, from_mount_path: List[str]) -> int:
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
                cur_path_id = self.get_fsrecord_id(dir_name, cur_path_id)
                self._save_path_cache(cur_path_id, cur_path)
        return cur_path_id

    def _update_caches(
            self, name_list: List[str], id_list: List[int], parent_path: str
    ) -> None:
        """Updates name and id caches using lists from get_path()."""
        if not name_list:
            return
        path_depth = len(name_list)
        name_list.reverse()
        id_list.reverse()
        start_idx = 0 if name_list[0] else 1
        for i in range(path_depth):
            self._save_path_cache(
                id_list[i],
                get_full_path(name_list[i],
                              (parent_path.split("/")
                               if parent_path
                               else []) + name_list[start_idx:i],
                              id_list[i - 1] if i > 0 else None))

    def get_path(self, fsrecord_id: int) -> str:
        """Returns path of given fsrecord_id."""
        name_list: List[str] = []
        id_list: List[int] = []
        current_id = fsrecord_id
        while current_id:
            cached_path = self._path_cache.get(current_id)
            if cached_path:
                self._update_caches(name_list, id_list, cached_path)
                return cached_path
            for row in self._exec_query(
                    _DIR_PARENT_SELECT, (current_id,), commit=False):
                logging.debug(row)
                name_list.append(row[1])
                id_list.append(current_id)
                current_id = row[0]
        self._update_caches(name_list, id_list, "")
        return self._path_cache[fsrecord_id]

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
