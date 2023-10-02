import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from file_database import FileManagerDatabase  # noqa: E402

_DB_SCHEMA = SCRIPT_DIR.parent / "fileManager_schema.sql"
_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "fileManager_test_dump.sql"
_REFERENCE_DB_NAME = "reference.db"
_TEST_DB_NAME = "test.db"
# Tables and indexes to ignore
_TABLE_SELECT = "SELECT `ROWID`, `{}`.* FROM `{}` ORDER BY `ROWID`"
_TABLE_COMPARE: Dict[str, List[int]] = {
    "types": [],
    "files": [3],
    "disks": [1, 2],
    "fsrecords": [4, 7],
}


def test_error_exec_sql(tmp_path: Path) -> None:
    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        with pytest.raises(sqlite3.OperationalError):
            db._exec_query(_TABLE_SELECT.format("foo", "foo"), ())


def test_error_missing_setup(tmp_path: Path) -> None:
    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        with pytest.raises(ValueError):
            db.set_top_dir()
        with pytest.raises(ValueError):
            db.set_cur_dir(tmp_path)
        with pytest.raises(ValueError):
            db.update_file("bar.txt")
        with pytest.raises(ValueError):
            db.clean_cur_dir(["foo.log", "bar.txt"], True)
        with pytest.raises(ValueError):
            db.clean_cur_dir(["foo", "bar"], False)


def test_set_disk_change(tmp_path: Path, mocker) -> None:
    def mock_exec_query(self, sql: str, params: Tuple, commit=True):
        yield [5, "abc", 500, "test-label"]

    mocker.patch(
        "file_database.FileManagerDatabase._exec_query", mock_exec_query)

    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        db.set_disk("abc", 400, "new-label")
        assert db._disk_id == 5
        assert db._disk_uuid == "abc"
        assert db._disk_size == 400
        assert db._disk_label == "new-label"


def create_db(db_path: Path, db_dump: Path) -> None:
    connection = sqlite3.connect(db_path)
    connection.executescript(db_dump.read_text())
    connection.close()


def dump_db(db_path: Path, db_dump: Path):
    connection = sqlite3.connect(db_path)
    with open(db_dump, 'w') as f:
        for line in connection.iterdump():
            f.write('%s\n' % line)
    connection.close()


def compare_db_with_ignores(db1_path: Path, dg2_path: Path) -> None:
    connection_1 = sqlite3.connect(db1_path)
    connection_2 = sqlite3.connect(dg2_path)
    for table, excludes in _TABLE_COMPARE.items():
        res_1 = connection_1.execute(_TABLE_SELECT.format(table, table))
        res_2 = connection_2.execute(_TABLE_SELECT.format(table, table))
        while True:
            rows_1 = res_1.fetchone()
            rows_2 = res_2.fetchone()
            if rows_1 is None and rows_2 is None:
                break
            if rows_1 is None or rows_2 is None:
                assert 0
            for i in range(len(rows_1)):
                if i not in excludes:
                    assert rows_1[i] == rows_2[i]
    connection_1.close()
    connection_2.close()


def test_update_dir(tmp_path):
    reference_db_path = tmp_path / _REFERENCE_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    test_db_path = tmp_path / _TEST_DB_NAME
    create_db(test_db_path, _DB_SCHEMA)
    with FileManagerDatabase(
            test_db_path, time.time()) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR, max_depth=None)
    dump_db(test_db_path, tmp_path / "test_update_dir.sql")
    compare_db_with_ignores(reference_db_path, test_db_path)


def test_update_dir_no_hash(tmp_path):
    reference_db_path = tmp_path / _REFERENCE_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    test_db_path = tmp_path / _TEST_DB_NAME
    create_db(test_db_path, _DB_SCHEMA)
    with FileManagerDatabase(
            test_db_path, time.time()) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR, max_depth=None)
    with FileManagerDatabase(
            test_db_path, time.time() - 3600) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR, max_depth=None)
    dump_db(test_db_path, tmp_path / "test_update_dir_no_hash.sql")
    compare_db_with_ignores(reference_db_path, test_db_path)


def test_update_dir_rerun(tmp_path):
    reference_db_path = tmp_path / _REFERENCE_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    test_db_path = tmp_path / _TEST_DB_NAME
    create_db(test_db_path, _DB_SCHEMA)
    with FileManagerDatabase(
            test_db_path, time.time()) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR, max_depth=None)
    with FileManagerDatabase(
            test_db_path, time.time() + 3600) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR, max_depth=None)
    dump_db(test_db_path, tmp_path / "test_update_dir_rerun.sql")
    compare_db_with_ignores(reference_db_path, test_db_path)


def test_delete_dir(tmp_path):
    reference_db_path = tmp_path / _REFERENCE_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(
            reference_db_path, time.time()) as file_db:
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
