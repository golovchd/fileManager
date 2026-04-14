from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from file_manager.db_utils import create_db
from file_manager.file_manager import FileUtils

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DB_DIR = SCRIPT_DIR.parent / "test_db"

_DB_TEST_DB_DUMP = [TEST_DATA_DB_DIR / "fileManager_test_dump.sql"]
_DB_TEST_DB_1 = [TEST_DATA_DB_DIR / "fileManager_test_1.sql"]
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

@pytest.mark.parametrize(
    "disk, path, recursive, result",
    [
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45", "home", False,
            (0, 0, 1),
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45", "home/dimagolov/git/fileManager/test_data/media", False,
            (9435024, 6, 0),
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45", "home/dimagolov/git/fileManager/test_data", True,
            (22849002, 12, 5),
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45", "home/dimagolov/git/fileManager/test_data/storage", True,
            (13413978, 6, 3),
        ),
    ]
)
def test_list_dir(tmp_path: Path, disk: str, path: str, recursive: bool, result: tuple[int, int, int]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        assert db.list_dir(disk, path, recursive) == result

@pytest.mark.parametrize(
    "path1, path2, result",
    [
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/storage/second_dir",
            "61BB-02E2/Data/storage/second_dir",
            0,
        ),
        (
            "61BB-02E2/Data/storage/second_dir",
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/storage/second_dir",
            0,
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/storage",
            "61BB-02E2/Data/storage",
            1,
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov",
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git",
            2,
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git",
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov",
            2,
        ),
        (
            "61BB-02E2/Data/storage",
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/storage",
            1,
        ),
        (
            "61BB-02E2/Data/storage",
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/media",
            7,
        ),
        (
            "0a2e2cb7-4543-43b3-a04a-40959889bd45/home/dimagolov/git/fileManager/test_data/media",
            "61BB-02E2/Data/storage",
            7,
        ),
    ]
)
def test_diff(tmp_path: Path, path1: str,  path2: str, result: int) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        assert db.diff(path1, path2) == result

@pytest.mark.parametrize(
    "disk, dir, name, include_path, exclude_path, size, expected_result",
    [
        (None, True, 'fileManager', [], [], '',
            [[5, 'fileManager', None, None, None, None, None, 4, 1, '', 'home/dimagolov/git']]
        ),
        (None, False, 'fileManager', [], [], '',
            []
        ),
        (None, False, 'IMG_0013.JPG', [], [], '',
            [[11, 'IMG_0013.JPG', 1.69543415966538858416e+09, 1.69559020981002426141e+09, 4, 3242996, 'f7131eaafe02290403d0c96837e69e99134749a2', 7, 1, '', 'home/dimagolov/git/fileManager/test_data/media']]
        ),
        (None, False, 'DSC06979c.JPG', [], [], '',
            [
                [17, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 14, 1, '', 'home/dimagolov/git/fileManager/test_data/storage'],
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        (None, False, 'DSC06979c.JPG', ['Data/*'], [], '',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        (None, False, 'DSC06979c.JPG', [], ['*dimagolov*'], '',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '+1215700',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '>1215700',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '+1215800',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '>1215800',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '-1215800',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '<1215800',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '-1215700',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '<1215700',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '1215763',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '<1215763',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '>1215763',
            []
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', ['Data/*'], [], '',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
        ('DG-5TB-4', False, 'DSC06979c.JPG', [], [], '',
            [
                [28, 'DSC06979c.JPG', 1.6954341597253854275e+09, 1.69559020993184852597e+09, 7, 1215763, 'c81bb242e63bb1296fa1062fe5b3118f476193c9', 26, 2, 'DG-5TB-4', 'Data/storage'],
            ]
        ),
    ]
)
def test_find(mocker, tmp_path: Path, disk: str, dir: bool, name: str, include_path: list[str], exclude_path: list[str], size: str, expected_result) -> None:
    def mock_print_find_results(find_results: list[list[Any]], print_sha: bool) -> None:
        assert find_results == expected_result

    mocker.patch(
        "file_manager.file_manager_implementation.print_find_results", mock_print_find_results)

    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_1)
    with FileUtils(reference_db_path) as db:
        db.find(disk, dir,  name, include_path, exclude_path, size, False)
