from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import (DB_SCHEMA, compare_db_with_ignores,  # noqa: E402
                      create_db, dump_db)
from file_database_update import FileDatabaseUpdater  # noqa: E402
from file_utils import FsClient  # noqa: E402
from storage_client import StorageClient  # noqa: E402

_DB_TEST_DB_DUMP = [SCRIPT_DIR.parent / "test_db/fileManager_test_dump.sql"]
_REFERENCE_DB_NAME = "reference.db"
_TEST_DB_NAME = "test.db"


# def test_update_dir(tmp_path):
#     reference_db_path = tmp_path / _REFERENCE_DB_NAME
#     create_db(reference_db_path, _DB_TEST_DB_DUMP)
#     test_db_path = tmp_path / _TEST_DB_NAME
#     create_db(test_db_path, DB_SCHEMA)
#     test_storage_dir = tmp_path / "storage"
#     copytree(TEST_DATA_DIR, test_storage_dir)
#     with FileDatabaseUpdater(
#             test_db_path, time.time(), 2, FsClient(test_storage_dir)) as new_file_db:
#         new_file_db.update_dir(max_depth=None)
#     dump_db(test_db_path, tmp_path / "test_update_dir.sql")
#     compare_db_with_ignores(reference_db_path, test_db_path)


# def test_update_dir_no_hash(tmp_path):
#     reference_db_path = tmp_path / _REFERENCE_DB_NAME
#     create_db(reference_db_path, _DB_TEST_DB_DUMP)
#     test_db_path = tmp_path / _TEST_DB_NAME
#     create_db(test_db_path, DB_SCHEMA)
#     test_storage_dir = tmp_path / "storage"
#     copytree(TEST_DATA_DIR, test_storage_dir)
#     with FileDatabaseUpdater(
#             test_db_path, time.time(), 2, FsClient(test_storage_dir)) as new_file_db:
#         new_file_db.update_dir(max_depth=None)
#     with FileDatabaseUpdater(
#             test_db_path, time.time() - 3600, 2, FsClient(test_storage_dir)) as new_file_db:
#         new_file_db.update_dir(max_depth=None)
#     dump_db(test_db_path, tmp_path / "test_update_dir_no_hash.sql")
#     compare_db_with_ignores(reference_db_path, test_db_path)


# def test_update_dir_rerun(tmp_path):
#     reference_db_path = tmp_path / _REFERENCE_DB_NAME
#     create_db(reference_db_path, _DB_TEST_DB_DUMP)
#     test_db_path = tmp_path / _TEST_DB_NAME
#     create_db(test_db_path, DB_SCHEMA)
#     test_storage_dir = tmp_path / "storage"
#     copytree(TEST_DATA_DIR, test_storage_dir)
#     with FileDatabaseUpdater(
#             test_db_path, time.time(), 2, FsClient(test_storage_dir)) as new_file_db:
#         new_file_db.update_dir(max_depth=None)
#     with FileDatabaseUpdater(
#             test_db_path, time.time() + 3600, 2, FsClient(test_storage_dir)) as new_file_db:
#         new_file_db.update_dir(max_depth=None)
#     dump_db(test_db_path, tmp_path / "test_update_dir_rerun.sql")
#     compare_db_with_ignores(reference_db_path, test_db_path)


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
