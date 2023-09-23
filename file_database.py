"""File database module."""

import logging
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import file_utils

DEFAULT_DATABASE_NAME = "fileManager.db"

_DISK_SELECT = ("SELECT `ROWID`, `UUID`, `DiskSize`, `Label`"
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

_FILE_SELECT = ("SELECT `ROWID`, `EarliestDate`, `CanonicalName`, "
                "`CanonicalType`, `MediaType` FROM `files` WHERE `SHA1` = ?")
_FILE_TIME_UPDATE = ("UPDATE `files` SET `EarliestDate` = ? WHERE "
                     "`EarliestDate` > ? AND `ROWID` = ? ")
_FILE_INSERT = ("INSERT INTO `files` (`FileSize`, `SHA1`, `EarliestDate`, "
                "`CanonicalName`, `CanonicalType`, `MediaType`) "
                "VALUES (?, ?, ?, ?, ?, ?)")
_FS_FILE_SELECT = ("SELECT `fsrecords`.`ROWID`, `fsrecords`.`SHA1ReadDate`, "
                   "`fsrecords`.`FileDate`, `files`.`ROWID`, "
                   "`files`.`FileSize`, `SHA1`"
                   " FROM `fsrecords` INNER JOIN `files`"
                   " ON `files`.`ROWID` = `fsrecords`.`FileId`"
                   " WHERE `DiskId` = ? AND `ParentId` = ? AND `Name` = ?"
                   " AND `FileId` IS NOT NULL")


class FileManagerDatabase:
    """Representation of fileManager database."""

    def __init__(
            self, db_name: str, rehash_time: int, new_update: bool = False):
        self._db_name = db_name
        self._rehash_time = rehash_time
        self._new_update = new_update
        self._con = None
        # Details on current disk
        self._disk_id = None
        self._disk_uuid = None
        self._disk_size = None
        self._disk_label = None
        # Ref to top dir on disk of the current dir
        self._top_dir_id: int = 0
        self._dir_id_cache: Dict[str, int] = {}
        self._cur_dir_id: int = 0
        self._cur_dir_path: Path = Path(".")
        logging.info(f"Using DB {db_name}")

    def __enter__(self):
        """Initiate connect to DB."""
        self._con = sqlite3.connect(self._db_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closing connect to DB."""
        del exc_type, exc_value, traceback
        if self._con:
            self._con.close()

    def _exec_query(self, sql, params, commit=True):
        """SQL quesy executor with logging."""
        try:
            result = self._con.execute(sql, params)
            if commit:
                self._con.commit()
            logging.debug("SQL succeed: %s with %r", sql, params)
            return result
        except sqlite3.OperationalError as e:
            logging.exception("SQL failed: %s with %r", sql, params)
            raise e

    def set_disk(self, uuid, size, label):
        """Create/update disk details in DB."""
        if self._disk_uuid == uuid:
            # TODO: support free disk space tracking in DB
            if self._disk_size != size or self._disk_label != label:
                self._exec_query(
                    _DISK_UPDATE_SIZE, (size, label, uuid), commit=False)
            return
        for row in self._exec_query(_DISK_SELECT, (uuid,), commit=False):
            self._disk_id = row[0]
            self._disk_uuid = row[1]
            self._disk_size = row[2]
            self._disk_label = row[3]
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
            self._dir_id_cache = {'': self._top_dir_id}
            break
        else:
            self._exec_query(_FSDIR_INSERT, ("", None, self._disk_id))
            self.set_top_dir()

    def get_media_type(self, file_type):
        """Returns media type from file type (extension)."""
        del file_type
        return None

    def get_file_id(self, fsrecord_name, file_details=None):
        """Getting file id by file's SHA1, inserting if missing."""
        if file_details:
            (file_name, file_type, size, mtime, sha1) = file_details
        else:
            (file_name, file_type, size, mtime, sha1) = file_utils.read_file(
                self._cur_dir_path / fsrecord_name, True)
        if not sha1:
            return 0, mtime

        logging.debug("get_file_id: name=%s, type=%s, %d, %d, %r",
                      file_name, file_type, size, mtime, sha1)
        for row in self._exec_query(_FILE_SELECT, (sha1,), commit=False):
            self._exec_query(_FILE_TIME_UPDATE, (mtime, mtime, row[0]))
            return row[0], mtime

        self._exec_query(_FILE_INSERT, (
            size, sha1, mtime, file_name, file_type,
            self.get_media_type(file_type)))
        return self.get_file_id(
            fsrecord_name, (file_name, file_type, size, mtime, sha1))

    def get_fsfile_id(self, fsrecord_name):
        """Getting fsrecord id for given file in current did,
            insert if missing."""
        if not self._cur_dir_id:
            raise ValueError("Missing _cur_dir_id")
        file_id, file_mtime = self.get_file_id(fsrecord_name)
        if not file_id:
            logging.warning("get_fsfile_id: failed to get SHA1/file_id for %s",
                            fsrecord_name)
            return 0
        for row in self._exec_query(
                _FSRECORD_SELECT.format("NOT"),
                (self._disk_id, self._cur_dir_id, fsrecord_name),
                commit=False):
            logging.debug(
                f"get_fsfile_id: {fsrecord_name}, parent {self._cur_dir_id} = "
                f"{row[0]}, SHA1 hash updated on %s",
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[1])))
            self._exec_query(_FSFILE_UPDATE,
                             (file_mtime, file_id, time.time(), row[0]))
            return row[0]

        self._exec_query(
            _FSFILE_INSERT,
            (fsrecord_name, self._cur_dir_id,
             self._disk_id, file_mtime, file_id, time.time()))
        return self.get_fsrecord_id(fsrecord_name, self._cur_dir_id, True)

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
        if not from_mount_path:
            return self._top_dir_id
        cached_id = self._dir_id_cache.get("/".join(from_mount_path))
        if cached_id:
            return cached_id

        cur_path_id = self._top_dir_id
        cur_path_list = []
        for dir_name in from_mount_path:
            cur_path_list.append(dir_name)
            cur_path = "/".join(cur_path_list)
            if cur_path in self._dir_id_cache:
                cur_path_id = self._dir_id_cache[cur_path]
            else:
                cur_path_id = self.get_fsrecord_id(dir_name, cur_path_id)
                self._dir_id_cache[cur_path] = cur_path_id
        return cur_path_id

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
        else:
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
                mtime, file_id, time.time(), fsrecord_id))
        else:
            self._exec_query(
                _FSFILE_INSERT,
                (file_name, self._cur_dir_id, self._disk_id, mtime, file_id,
                 time.time()))

    def update_file(self, file_full_name: str) -> None:
        """Updates or inserts in DB details on file."""
        if not self._cur_dir_id:
            raise ValueError("Missing _cur_dir_id")
        (
            fsrecord_id, sha1_read_date, db_file_mtime, file_id, db_file_size,
            db_file_sha1
        ) = self.get_db_file_info(file_full_name)
        file_path = self._cur_dir_path / file_full_name
        (file_name, file_type, size, mtime, _) = file_utils.read_file(
                file_path, False)
        if (self._rehash_time < sha1_read_date and
                db_file_size == size and db_file_mtime == mtime):
            return  # Current file details matching DB, no need to re-hash

        (file_name, file_type, size, mtime, sha1) = file_utils.read_file(
                file_path, True)
        new_file_id = self.select_update_file_record(
            sha1, mtime, size, file_name, file_type
        )
        self.update_fsrecord(fsrecord_id, file_full_name, mtime, new_file_id)
        # TODO: Handle deletion of old `files` record file_id
        del file_id

    def update_files(self, files: List[str]) -> None:
        """Updating files in current dir."""
        for file_name in files:
            if (self._cur_dir_path / file_name).is_symlink():
                logging.warning("update_files called for symlink %s",
                                self._cur_dir_path / file_name)
                continue
            if self._new_update:
                self.update_file(file_name)
            else:
                self.get_fsfile_id(file_name)

    def update_dir(
            self, path: Path,
            max_depth: Optional[int] = 0) -> None:
        """Updating DB with dir details, entrypoint for update_database."""
        dir_path = file_utils.get_full_dir_path(path)
        logging.debug(f"update_dir for: {path}, full path: {dir_path}")
        self.set_disk(**file_utils.get_path_disk_info(dir_path))
        self.set_cur_dir(dir_path)
        files, sub_dirs = file_utils.read_dir(dir_path)
        self.clean_cur_dir(files, is_files=True)
        self.clean_cur_dir(sub_dirs, is_files=False)
        self.update_files(files)

        if max_depth != 0:
            for sub_dir_name in sub_dirs:
                if (path / sub_dir_name).is_symlink():
                    logging.warning("update_dir called for symlink %s",
                                    self._cur_dir_path / sub_dir_name)
                    continue
                self.update_dir(path / sub_dir_name,
                                max_depth - 1 if max_depth else None)
