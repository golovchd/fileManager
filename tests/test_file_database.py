import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import TABLE_SELECT, create_db  # noqa: E402
from file_database import FileManagerDatabase  # noqa: E402

_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "fileManager_test_dump.sql"
_DB_TEST_DB_1 = SCRIPT_DIR.parent / "fileManager_test_1.sql"
_TEST_DB_NAME = "test.db"


def test_error_exec_sql(tmp_path: Path) -> None:
    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        with pytest.raises(sqlite3.OperationalError):
            db._exec_query(TABLE_SELECT.format("foo", "foo"), ())


def test_set_disk_change(tmp_path: Path, mocker) -> None:
    def mock_exec_query(self, sql: str, params: Tuple, commit=True):
        yield [5, "abc", 500, "test-label"]

    mocker.patch(
        "file_database.FileManagerDatabase._exec_query", mock_exec_query)

    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        db.set_disk("abc", 500, "test-label")
        assert db._disk_id == 5
        assert db._disk_uuid == "abc"
        assert db._disk_size == 500
        assert db._disk_label == "test-label"

    with pytest.raises(ValueError) as error_info:
        with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
            db.set_disk("abc", 400, "test-label")
        assert (
            error_info ==
            "Disk UUID abc details changed: 500->400, test-label->test-label")

    with pytest.raises(ValueError) as error_info:
        with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
            db.set_disk("abc", 500, "new-label")
        assert (
            error_info ==
            "Disk UUID abc details changed: 500->500, test-label->new-label")


@pytest.mark.parametrize(
    "fsrecord_id, fsrecord_path, dir_cache, id_cache",
    [
        (
            2,
            "home",
            {1: "", 2: "home"},
            {"": 1, "home": 2}
        ),
        (
            7,
            "home/dimagolov/git/fileManager/test_data/media",
            {
                1: "",
                2: "home",
                3: "home/dimagolov",
                4: "home/dimagolov/git",
                5: "home/dimagolov/git/fileManager",
                6: "home/dimagolov/git/fileManager/test_data",
                7: "home/dimagolov/git/fileManager/test_data/media",
            },
            {
                "": 1,
                "home": 2,
                "home/dimagolov": 3,
                "home/dimagolov/git": 4,
                "home/dimagolov/git/fileManager": 5,
                "home/dimagolov/git/fileManager/test_data": 6,
                "home/dimagolov/git/fileManager/test_data/media": 7,
            }
        ),
        (
            20,
            "home/dimagolov/git/fileManager/test_data/storage/tagged",
            {
                1: "",
                2: "home",
                3: "home/dimagolov",
                4: "home/dimagolov/git",
                5: "home/dimagolov/git/fileManager",
                6: "home/dimagolov/git/fileManager/test_data",
                14: "home/dimagolov/git/fileManager/test_data/storage",
                20: "home/dimagolov/git/fileManager/test_data/storage/tagged",
            },
            {
                "": 1,
                "home": 2,
                "home/dimagolov": 3,
                "home/dimagolov/git": 4,
                "home/dimagolov/git/fileManager": 5,
                "home/dimagolov/git/fileManager/test_data": 6,
                "home/dimagolov/git/fileManager/test_data/storage": 14,
                "home/dimagolov/git/fileManager/test_data/storage/tagged": 20,
            }
        ),
    ],
)
def test_get_path(
        tmp_path: Path, fsrecord_id: int, fsrecord_path: str,
        dir_cache: Dict[int, str], id_cache: Dict[str, int]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        db.set_disk("0a2e2cb7-4543-43b3-a04a-40959889bd45", 59609420, "")
        assert db.get_path(fsrecord_id) == fsrecord_path
        assert db._path_cache == dir_cache
        assert db._id_cache[1] == id_cache


@pytest.mark.parametrize(
    "dir, recursive, subdirs",
    [
        (1, False, [2]),
        (6, False, [7, 14]),
        (6, True, [7, 14, 18, 20]),
    ]
)
def test_query_subdirs(
        tmp_path: Path, dir: int, recursive: bool, subdirs: List[int]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        db.set_disk("0a2e2cb7-4543-43b3-a04a-40959889bd45", 59609420, "")
        assert db.query_subdirs(dir, recursive) == subdirs


@pytest.mark.parametrize(
    "file_id, disks, parent_root_path, expected_result",
    [
        (["2", "7"], ["0a2e2cb7-4543-43b3-a04a-40959889bd45"], None, {"": [
                "home/dimagolov/git/fileManager/test_data/media/DSC06979.JPG",
                "home/dimagolov/git/fileManager/test_data/storage/DSC06979 (copy).JPG",
                "home/dimagolov/git/fileManager/test_data/storage/DSC06979.JPG",
                "home/dimagolov/git/fileManager/test_data/storage/DSC06979c.JPG",
        ]}),
        ([], ["0a2e2cb7-4543-43b3-a04a-40959889bd45"], None, {})
    ]
)
def test_get_file_path_on_disk(tmp_path: Path, file_id: List[str], disks: List[str], parent_root_path: Optional[str], expected_result: Dict[str, List[str]]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        db.set_disk("0a2e2cb7-4543-43b3-a04a-40959889bd45", 59609420, "")
        assert db.get_file_path_on_disk(file_id, disks, parent_root_path=parent_root_path) == expected_result


@pytest.mark.parametrize(
    "disk_name, disk_id",
    [
        ("0a2e2cb7-4543-43b3-a04a-40959889bd45", 1),
        ("DG-5TB-4", 2),
        ("61BB-02E2", 2),
    ]
)
def test_set_disk_by_name(tmp_path: Path, disk_name: str, disk_id: int) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        db.set_disk_by_name(disk_name)
        assert db._disk_id == disk_id

def test_failure_set_disk_by_name(tmp_path: Path) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        with pytest.raises(ValueError):
            db.set_disk_by_name("NO-SUCH-DISK")


@pytest.mark.parametrize(
    "path, dir_id",
    [
        ("home/dimagolov/git/fileManager/test_data/media", 7),
        ("home/dimagolov/git/fileManager/test_data/storage", 14),
        ("not existing path", 0),
    ]
)
def test_get_path_on_disk(tmp_path: Path, path: str, dir_id: int) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        assert db.get_path_on_disk("0a2e2cb7-4543-43b3-a04a-40959889bd45", path) == dir_id


@pytest.mark.parametrize(
    "disk_name, confirm, result",
    [
        ("0a2e2cb7-4543-43b3-a04a-40959889bd45", True, 0),
        ("DG-5TB-4", True, 2),
        ("DG-5TB-4x", True, 1),
        ("61BB-02E2", True, 0),
        ("61BB-02E2", False, 3),
    ]
)
def test_delete_disk_errors(tmp_path: Path, mocker, disk_name: str, confirm: bool, result: int) -> None:
    mocker.patch('file_database.file_utils.get_confirmation', lambda x,y: confirm)
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        assert db.delete_disk(disk_name, False, False) == result


def test_delete_disk_force(tmp_path: Path, mocker) -> None:
    mocker.patch('file_database.file_utils.get_confirmation', lambda x,y: False)
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        assert db.delete_disk("61BB-02E2", False, True) == 0
