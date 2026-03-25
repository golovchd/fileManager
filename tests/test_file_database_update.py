from __future__ import annotations

import time
from pathlib import Path

import pytest

from file_manager.db_utils import DB_SCHEMA, create_db, dump_db
from file_manager.file_database_update import (FileDatabaseUpdater,
                                               IntegrityError)
from file_manager.file_utils import FsClient
from file_manager.storage_client import StorageClient

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"

_DB_TEST_DB_DUMP = [SCRIPT_DIR.parent / "test_db/fileManager_test_dump.sql"]
_REFERENCE_DB_NAME = "reference.db"
_TEST_DB_NAME = "test.db"


def test_delete_dir(tmp_path):
    reference_db_path = tmp_path / _REFERENCE_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileDatabaseUpdater(
            reference_db_path, time.time(), 1, StorageClient(str(tmp_path))) as file_db:
        file_db.set_disk('0a2e2cb7-4543-43b3-a04a-40959889bd45', 59609420, '')
        file_db._exec_query("DELETE FROM `fsrecords` WHERE `Name` = ?",
                            ("storage",), commit=True)
        file_db.handle_orfans(clear_orfan_files=True)
        dump_db(reference_db_path, tmp_path / "test_delete_dir_fsrecors.sql")
        for row in file_db._exec_query(
                "SELECT COUNT(*) FROM `fsrecords` WHERE `FileId` IS NOT NULL",
                ()):
            assert row[0] == 6
            break
        else:
            raise ValueError("Failed to count records in `fsrecords`")
        for row in file_db._exec_query("SELECT COUNT(*) FROM `files`", ()):
            assert row[0] == 6
            break
        else:
            raise ValueError("Failed to count records in `files`")


def test_error_missing_setup(tmp_path: Path) -> None:
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, StorageClient(str(tmp_path))) as db:
        with pytest.raises(ValueError):
            db.set_top_dir()
        with pytest.raises(ValueError):
            db.set_cur_dir()
        with pytest.raises(ValueError):
            db.update_file("bar.txt", 0, 0, 0, 0, 0, "")
        with pytest.raises(ValueError):
            db.clean_cur_dir(["foo.log", "bar.txt"], True)
        with pytest.raises(ValueError):
            db.clean_cur_dir(["foo", "bar"], False)


def test_update_file_skip_symlink(tmp_path: Path) -> None:
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, FsClient(str(TEST_DATA_DIR))) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        assert db.update_file("IMG_0013_link.JPG", 0, 0, 0, 0, 0, "") == (0, 0, 0)


def test_update_file_skip_slow_db_file(tmp_path: Path, mocker) -> None:
    mocker.patch("file_manager.file_utils.FsClient.slow_file_read", lambda: True)
    storage_client = FsClient(str(TEST_DATA_DIR))

    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        assert db.update_file("IMG_0013.JPG", 6, 100, 100, 7, 1024, "abc") == (0, 1024, 0)


def test_update_file_skip_empty_files(tmp_path: Path) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))

    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "storage/second_dir"
        assert db.update_file("foo.txt", 0, 0, 0, 0, 0, "") == (0, 0, 0)


def test_update_file_skip_hashed(tmp_path: Path) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    cur_time = time.time()
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, cur_time - 10000, 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        test_file = "IMG_0013.JPG"
        _, _, size, mtime, _, _ = storage_client.read_file_info(test_file)
        assert db.update_file(test_file, 6, cur_time - 5000, mtime, 7, size, "abc") == (0, size, 0)


def test_update_file_skip_failed_to_hash(tmp_path: Path, mocker) -> None:
    mocker.patch("file_manager.file_utils.generate_file_sha1", lambda _: ("", 0))
    storage_client = FsClient(str(TEST_DATA_DIR))
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        test_file = "IMG_0013.JPG"
        _, _, size, mtime, _, _ = storage_client.read_file_info(test_file)
        assert db.update_file(test_file, 0, 0, 0, 0, 0, "") == (0, size, 0)


def test_update_file_db_insert(tmp_path: Path) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        test_file = "IMG_0013.JPG"
        _, _, file_size, _, _, _ = storage_client.read_file_info(test_file)
        hashed_size, size, hash_time = db.update_file(test_file, 0, 0, 0, 0, 0, "")
        assert hashed_size == size
        assert file_size == size
        assert hash_time > 0


def test_update_file_db_insert_skip_on_error(tmp_path: Path, mocker) -> None:
    def raise_integrity_error(*args, **kwargs):
        raise IntegrityError("Simulated integrity error")
    mocker.patch("file_manager.db_utils.SQLite3connection._exec_query", raise_integrity_error)
    storage_client = FsClient(str(TEST_DATA_DIR))
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        test_file = "IMG_0013.JPG"
        _, _, file_size, _, _, _ = storage_client.read_file_info(test_file)
        hashed_size, size, hash_time = db.update_file(test_file, 0, 0, 0, 0, 0, "")
        assert hashed_size == size
        assert file_size == size
        assert hash_time > 0


def test_update_files_skip_empty(tmp_path: Path, mocker) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        assert db.update_files([]) == (0, 0, 0)


def test_update_files_empty_db(tmp_path: Path) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        db.storage_client.cur_path = TEST_DATA_DIR / "media"
        test_file = "IMG_0013.JPG"
        _, _, file_size, _, _, _ = storage_client.read_file_info(test_file)
        hashed_size, size, hash_time = db.update_files([test_file])
        assert hashed_size == file_size
        assert size == file_size
        assert hash_time > 0


def test_update_files_mock_db(tmp_path: Path, mocker) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    storage_client.cur_path = TEST_DATA_DIR / "media"
    test_file = "IMG_0013.JPG"
    _, _, file_size, _, _, _ = storage_client.read_file_info(test_file)
    mocker.patch("file_manager.file_database.FileManagerDatabase.get_db_file_info", lambda _: {test_file: (6, 100, 100, 7, 1024, "abc")})
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 1, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        hashed_size, size, hash_time = db.update_files([test_file])
        assert hashed_size == file_size
        assert size == file_size
        assert hash_time > 0


def test_update_files_empty_db_threads(tmp_path: Path, mocker) -> None:
    storage_client = FsClient(str(TEST_DATA_DIR))
    storage_client.cur_path = TEST_DATA_DIR / "media"
    test_file = "IMG_0013.JPG"
    _, _, file_size, _, _, _ = storage_client.read_file_info(test_file)
    test_file2 = "IMG_0004.JPG"
    _, _, file_size2, _, _, _ = storage_client.read_file_info(test_file2)
    total_size = file_size + file_size2
    with FileDatabaseUpdater(tmp_path / _TEST_DB_NAME, time.time(), 4, storage_client) as db:
        db._cur_dir_id = 5
        db._cur_dir_path = "test_data/test_dir"
        hashed_size, size, hash_time = db.update_files([test_file, test_file2])
        assert hashed_size == total_size
        assert size == total_size
        assert hash_time > 0
