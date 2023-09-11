"""File database module."""

import logging
import os
import sqlite3

import file_utils

_DISK_SELECT = 'SELECT `ROWID`, `UUID` FROM `disks` WHERE UUID = ?'
_DISK_UPDATE_SIZE = ('UPDATE `disks` SET `DiskSize` = ?, `Label` = ?'
                     ' WHERE `ROWID` = ?')
_DISK_INSERT = ('INSERT INTO `disks` (`UUID`, `DiskSize`, `Label`) '
                'VALUES (?, ?, ?)')

_TOP_DIR_SELECT = ('SELECT `ROWID` FROM `fsrecords` WHERE `DiskId` = ? AND'
                   ' `ParentId` IS NULL AND `Name` = "" AND `FileId` IS NULL')
_FSRECORD_SELECT = ('SELECT `ROWID` FROM `fsrecords` WHERE `DiskId` = ? AND'
                    ' `ParentId` = ? AND `Name` = ? AND `FileId` IS {} NULL')
_FSDIR_INSERT = ('INSERT INTO `fsrecords` (`Name`, `ParentId`, `DiskId`) '
                 'VALUES (?, ?, ?)')
_FSFILE_INSERT = ('INSERT INTO `fsrecords` (`Name`, `ParentId`, `DiskId`, '
                  '`FileDate`, `FileId`) VALUES (?, ?, ?, ?, ?)')
_FSFILE_UPDATE = ('UPDATE `fsrecords` SET `FileDate` = ?, `FileId` = ? '
                  'WHERE `ROWID` = ?')
_DIR_CLEAN = ('DELETE FROM `fsrecords` WHERE `DiskId` = ? AND'
              ' `ParentId` = ? AND `FileId` IS {} NULL')
_DIR_CLEAN_NAMES = ' AND `Name` NOT IN (?'

_FILE_SELECT = 'SELECT `ROWID` FROM `files` WHERE `MD5` = ?'
_FILE_TIME_UPDATE = ('UPDATE `files` SET `EarliestDate` = ? WHERE '
                     '`EarliestDate` > ? AND `ROWID` = ? ')
_FILE_INSERT = ('INSERT INTO `files` (`FileSize`, `MD5`, `EarliestDate`, '
                '`CanonicalName`, `CanonicalType`, `MediaType`) '
                'VALUES (?, ?, ?, ?, ?, ?)')

class FileManagerDatabase(object):
  """Representation of fileManager database."""

  def __init__(self, db_name='fileManager.db'):
    self._db_name = db_name
    self._con = None
    self._disk_id = None
    self._disk_uuid = None
    self._top_dir_id = None
    self._cur_dir_id = None
    self._cur_dir_path = None
    log_format = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=log_format, level=logging.DEBUG)
    logging.info('Using DB %s', db_name)

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
      logging.debug('SQL succeed: %s with %r', sql, params)
      return result
    except sqlite3.OperationalError:
      logging.exception('SQL failed: %s with %r', sql, params)
      raise

  def set_disk(self, uuid, size, label):
    """Create/update disk details in DB."""
    if self._disk_uuid == uuid:
      self._exec_query(_DISK_UPDATE_SIZE, (size, label, uuid), commit=False)
      return
    for row in self._exec_query(_DISK_SELECT, (uuid,), commit=False):
      self._disk_id = row[0]
      self._disk_uuid = row[1]
      self.set_top_dir()
      break
    else:
      self._exec_query(_DISK_INSERT, (uuid, size, label))
      self.set_disk(uuid, size, label)

  def set_top_dir(self):
    """Loading from DB or creating if missing top dir id."""
    if not self._disk_id:
      raise ValueError('Missing _disk_id')
    for row in self._exec_query(
        _TOP_DIR_SELECT, (self._disk_id,), commit=False):
      self._top_dir_id = row[0]
      break
    else:
      self._exec_query(_FSDIR_INSERT, ('', None, self._disk_id))
      self.set_top_dir()

  def get_media_type(self, file_type):
    """Returns media type from file type (extension)."""
    del file_type
    return None

  def get_file_id(self, fsrecord_name, file_details=None):
    """Getting file id by file's MD5, inserting if missing."""
    if file_details:
      (file_name, file_type, size, mtime, md5) = file_details
    else:
      (file_name, file_type, size, mtime, md5) = file_utils.read_file(
          os.path.join(self._cur_dir_path, fsrecord_name))
    logging.debug('get_file_id: name=%s, type=%s, %d, %d, %r',
                  file_name, file_type, size, mtime, md5)
    for row in self._exec_query(_FILE_SELECT, (md5,), commit=False):
      self._exec_query(_FILE_TIME_UPDATE, (mtime, mtime, row[0]))
      return row[0], mtime

    self._exec_query(_FILE_INSERT, (size, md5, mtime, file_name, file_type,
                                    self.get_media_type(file_type)))
    return self.get_file_id(
        fsrecord_name, (file_name, file_type, size, mtime, md5))

  def get_fsfile_id(self, fsrecord_name):
    """Getting fsrecord id for given file in current did, insert if missing."""
    if not self._cur_dir_id:
      raise ValueError('Missing _cur_dir_id')
    file_id, file_mtime = self.get_file_id(fsrecord_name)
    for row in self._exec_query(
        _FSRECORD_SELECT.format('NOT'),
        (self._disk_id, self._cur_dir_id, fsrecord_name),
        commit=False):
      logging.debug('get_fsfile_id: %s, parent %r = %r',
                    fsrecord_name, self._cur_dir_id, row[0])
      self._exec_query(_FSFILE_UPDATE, (file_mtime, file_id, row[0]))
      return row[0]

    self._exec_query(
        _FSFILE_INSERT,
        (fsrecord_name, self._cur_dir_id, self._disk_id, file_mtime, file_id))
    return self.get_fsrecord_id(fsrecord_name, self._cur_dir_id, True)

  def get_fsrecord_id(self, fsrecord_name, parent_id, is_file=False):
    """Getting dir id for given name/parent, generating if missing."""
    if not self._disk_id:
      raise ValueError('Missing _disk_id')
    for row in self._exec_query(
        _FSRECORD_SELECT.format('NOT' if is_file else ''),
        (self._disk_id, parent_id, fsrecord_name),
        commit=False):
      logging.debug('get_fsrecord_id: %s, parent %r = %r, is_file=%r',
                    fsrecord_name, parent_id, row[0], is_file)
      return row[0]

    if is_file:
      raise ValueError('Missing fsrecord_id for a file')

    self._exec_query(_FSDIR_INSERT, (fsrecord_name, parent_id, self._disk_id))
    return self.get_fsrecord_id(fsrecord_name, parent_id, is_file=is_file)

  def set_cur_dir(self, dir_path):
    """Saving/updating dir with a path to disk root."""
    if not self._top_dir_id:
      raise ValueError('Missing _top_dir_id')
    from_mount_path = file_utils.get_path_from_mount(dir_path)
    logging.debug('set_cur_dir: %s as %r', dir_path, from_mount_path)
    parent_id = self._top_dir_id
    for dir_name in from_mount_path:
      parent_id = self.get_fsrecord_id(dir_name, parent_id)
    self._cur_dir_id = parent_id
    self._cur_dir_path = dir_path
    logging.debug('set_cur_dir: %s=%r', self._cur_dir_path, self._cur_dir_id)

  def clean_cur_dir(self, names, is_files):
    """Deleting from DB files/subdirs missing in names list."""
    if not self._cur_dir_id:
      raise ValueError('Missing _cur_dir_id')
    clean_sql = _DIR_CLEAN.format('NOT' if is_files else '')
    if names:
      clean_sql += _DIR_CLEAN_NAMES + ', ?' * (len(names) - 1) + ')'
    self._exec_query(
        clean_sql, tuple([self._disk_id, self._cur_dir_id] + names))

  def update_files(self, files):
    """Updating files in current dir."""
    for file_name in files:
      self.get_fsfile_id(file_name)

  def update_dir(self, path, max_depth=0):
    """Updating DB with dir details."""
    dir_path = file_utils.get_full_dir_path(path)
    logging.debug('update_dir for: %s, full path: %s', path, dir_path)
    self.set_disk(**file_utils.get_path_disk_info(dir_path))
    self.set_cur_dir(dir_path)
    files, sub_dirs = file_utils.read_dir(dir_path)
    self.clean_cur_dir(files, is_files=True)
    self.clean_cur_dir(sub_dirs, is_files=False)
    self.update_files(files)

    if max_depth != 0:
      for sub_dir_name in sub_dirs:
        self.update_dir(os.path.join(path, sub_dir_name),
                        max_depth - 1 if max_depth else None)
    