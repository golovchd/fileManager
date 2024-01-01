import logging
import sys
from pathlib import Path
from typing import Dict

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR.parent))

from duplicates_cleanup import LEFT_DIR_KEEP_ACTION  # noqa: E402
from duplicates_cleanup import RIGHT_DIR_KEEP_ACTION  # noqa: E402
from duplicates_cleanup import SKIP_ACTION, DuplicatesCleanup  # noqa: E402

TEST_CONFIG = SCRIPT_DIR.parent / "duplicates_cleanup.yaml"


@pytest.mark.parametrize(
    "dir_a, dir_b, action",
    [
        ("Data/Backup/1GB_1",
         "Data/Photos/! To Process/2014/01/2014-01-14",
         RIGHT_DIR_KEEP_ACTION),
        ("Data/Photos/2014",
         "Data/Photos/! To Process/!!/blah",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2015",
         "Data/Photos/! To Process/!!/blah",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Videos/2014",
         "Data/Videos/! To Process/!!/blah",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/1GB_1",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/test DropBox",
         SKIP_ACTION),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/test DropBox/tmp",
         SKIP_ACTION),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/DropBox/tmp",
         SKIP_ACTION),
        ("Data/Backup/2014-01-14 left",
         "Data/Backup/2014-01-15 right",
         RIGHT_DIR_KEEP_ACTION),
        ("Data/Backup/2015-01-14 left",
         "Data/Backup/2014-01-15 right",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos",
         "Data/Videos",
         RIGHT_DIR_KEEP_ACTION),
        ("Data/Anything",
         "5TB-2/Data/Photos",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Backup/2015-01-20 left",
         "5TB-2/Data/Backup/2014-01-15 right",
         LEFT_DIR_KEEP_ACTION),
        ("tmp/Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/1GB_1",
         SKIP_ACTION),
        ("Data/Backup/1GB_1",
         "tmp/Data/Photos/! To Process/2014/01/2014-01-14",
         SKIP_ACTION),
        ("Data/Photos/! To DVD/",
         "Data/Photos/! To Process/",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2014/2014-01-14",
         "Data/Photos/! To Process/2014",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To DVD/2011/2011-11-05-2/[Originals]",
         "Data/Photos/Family/Tetiana Skurchynska/[Originals]",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2011/2011-11-05-2/[Originals]",
         "Data/Photos/Family/Tetiana Skurchynska/[Originals]",
         LEFT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2011/2011-11-05-2/[Originals]",
         "Data/Photos/! To DVD/2011/2011-11-05-2/[Originals]",
         SKIP_ACTION),
        ("Data/Videos/! To Process/2014/2014-01-14",
         "Data/Videos/! To Process/2014",
         LEFT_DIR_KEEP_ACTION),
    ],
)
def test_multiple_rules(caplog, dir_a: str, dir_b: str, action: str):
    caplog.set_level(logging.DEBUG)
    config = DuplicatesCleanup(TEST_CONFIG)
    assert config.select_dir_to_keep(dir_a, dir_b) == action


@pytest.mark.parametrize(
    "dir_a, dir_b",
    [
        ("Data/Backup/1GB_1",
         "Data/Photos/! To Process/2014/01/2014-01-14"),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/1GB_1"),
        ("tmp/Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/1GB_1"),
    ],
)
def test_no_config(caplog, dir_a: str, dir_b: str):
    caplog.set_level(logging.DEBUG)
    config = DuplicatesCleanup(
            SCRIPT_DIR.parent / "missing.yaml")
    assert config.select_dir_to_keep(dir_a, dir_b) == SKIP_ACTION


@pytest.mark.parametrize(
    "names, index",
    [
        ({1: "aaa", 2: "bbb"}, 0),
        ({1: "aaa.jpg", 2: "bbb.jpg"}, 0),
        ({1: "aaa.jpg", 2: "aaa-1.jpg", 3: "aaa-2.jpg", 4: "aaa-3.jpg"}, 1),
        ({1: "aaa-1.jpg", 2: "aaa.jpg", 3: "aaa-2.jpg", 4: "aaa-3.jpg"}, 2),
        ({1: "aaa-1.jpg", 2: "aaa-2.jpg", 3: "aaa-3.jpg", 4: "aaa.jpg"}, 4),
        ({1: "aaa.jpg", 2: "aaa(1).jpg", 3: "aaa(2).jpg", 4: "aaa(3).jpg"}, 1),
        ({1: "aaa(1).jpg", 2: "aaa.jpg", 3: "aaa(2).jpg", 4: "aaa(3).jpg"}, 2),
        ({1: "aaa(1).jpg", 2: "aaa(2).jpg", 3: "aaa(3).jpg", 4: "aaa.jpg"}, 4),
        ({1: "123cam.jpg", 2: "123.jpg"}, 1),
        ({1: "123cam.jpg", 2: "321.jpg"}, 0),
        ({1: "aaa.jpg.bak", 2: "bbb.jpg"}, 0),
        ({1: "bbb.jpg.bak", 2: "bbb.jpg"}, 2),
        ({1: "bbb.jpg", 2: "bbb.jpg.bak"}, 1),
        ({1: "bbb.bak", 2: "bbb.jpg"}, 2),
        ({1: "bbb.jpg", 2: "bbb.bak"}, 1),
        ({1: "aaa.jpg.tmp", 2: "bbb.jpg"}, 0),
        ({1: "bbb.jpg.tmp", 2: "bbb.jpg"}, 2),
        ({1: "bbb.jpg", 2: "bbb.jpg.tmp"}, 1),
        ({1: "bbb.tmp", 2: "bbb.jpg"}, 2),
        ({1: "bbb.jpg", 2: "bbb.tmp"}, 1),
        ({1: "bbb.jpg", 2: "bbb.jpg.xyz"}, 1),
        ({1: ".com.google.Chrome.C4hlwU", 2: "abc.pdf"}, 2),
    ],
)
def test_select_file_to_keep(caplog, names: Dict[int, str], index: int):
    caplog.set_level(logging.DEBUG)
    config = DuplicatesCleanup(TEST_CONFIG)
    assert config.select_file_to_keep(names) == index


def test_bad_files_dict(caplog):
    caplog.set_level(logging.DEBUG)
    config = DuplicatesCleanup(TEST_CONFIG)
    with pytest.raises(IndexError):
        config.select_file_to_keep({0: "dummy"})
