import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import TABLE_SELECT, create_db  # noqa: E402
from file_manager import FileUtils

_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "fileManager_test_dump.sql"
_DB_TEST_DB_1 = SCRIPT_DIR.parent / "fileManager_test_1.sql"
_TEST_DB_NAME = "test.db"


@pytest.mark.parametrize(
    "dir, sort_index, result",
    [
        (20, -1, ([
            (21, "DSC06979.JPG",1.69543415974938416483e+09,1.69559021002043628688e+09, 9, 2902816, "db5da8c807516b52f961ea7df4abe8160943f619"),
            (22, "IMG_0004.JPG",1.69543415976938295361e+09,1.69559021004032182697e+09, 10, 2348583, "86463569661fe9bad6cfbd15e8cb8b5552d5fc90"),
        ],
        5251399, 2, 0)),
        (20, 6, ([
            (22, "IMG_0004.JPG",1.69543415976938295361e+09,1.69559021004032182697e+09, 10, 2348583, "86463569661fe9bad6cfbd15e8cb8b5552d5fc90"),
            (21, "DSC06979.JPG",1.69543415974938416483e+09,1.69559021002043628688e+09, 9, 2902816, "db5da8c807516b52f961ea7df4abe8160943f619"),
        ],
        5251399, 2, 0)),
        (6, -1, ([
            (7, "media",None,None, None, None, None),
            (14, "storage",None,None, None, None, None),
        ],
        0, 0, 2)),
        (6, 6, ([
            (7, "media",None,None, None, None, None),
            (14, "storage",None,None, None, None, None),
        ],
        0, 0, 2)),
    ]
)
def test_get_dir_content(
        tmp_path: Path, dir: int, sort_index: int, result: tuple[list[tuple[int, str, float, float, int, int, str]], int, int, int]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileUtils(reference_db_path) as db:
        assert db._get_dir_content(dir, sort_index) == result

@pytest.mark.parametrize(
    "row, result",
    [
        (
            (7, "media",None,None, None, None, None),
            [(7, "media",None,None, None, None, None)]
        ),
        (
            (21, "DSC06979.JPG",1.69543415974938416483e+09,1.69559021002043628688e+09, 9, 2902816, "db5da8c807516b52f961ea7df4abe8160943f619"),
            []
        ),
        (
            (23, "empty_dir",None,None, None, None, None),
            []
        ),
    ]
)
def test_get_nonempty_dir_row(tmp_path: Path, row: tuple[int, str, float, float, int, int, str], result: list[Any]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        assert db._get_nonempty_dir_row(row) == result


@pytest.mark.parametrize(
    "disk_path, result",
    [
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov",
            (1, 3, "0a2e2cb7-4543-43b3-a04a-40959889bd45"),
        ),
    ]
)
def test_get_disk_dir_id(tmp_path: Path, disk_path: str, result: tuple[int, int, str]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        assert db.get_disk_dir_id(disk_path) == result

@pytest.mark.parametrize(
    "disk_path, message",
    [
        (
            "DG-5TB-1/home/dimagolov",
            "Failed to find disk DG-5TB-1 in database"
        ),
        (
            "DG-5TB-4/home/dimagolov",
            "Disk param DG-5TB-4 returns more than one disk with UUIDs 61BB-02E2,49a043bf-7957-4bad-b502-95a985e27f60 from database"
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/test",
            "Missing fsrecord_id for a dir test under 2"
        ),
    ]
)
def test_get_disk_dir_id_error(tmp_path: Path, disk_path: str, message: str) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        with pytest.raises(ValueError, match=message):
            db.get_disk_dir_id(disk_path)
