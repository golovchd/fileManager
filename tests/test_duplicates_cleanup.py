import sys
from pathlib import Path

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
        ("Data/Backup/Torus/2015-12-03 DropBox/Torus Management/results",
         "Data/Photos/! To Process/2014/01/2014-01-14",
         SKIP_ACTION),
        ("Data/Backup/1GB_1",
         "Data/Photos/! To Process/2014/01/2014-01-14",
         RIGHT_DIR_KEEP_ACTION),
        ("Data/Photos/! To Process/2014/01/2014-01-14",
         "Data/Backup/1GB_1",
         LEFT_DIR_KEEP_ACTION),
    ],
)
def test_multiple_rules(dir_a: str, dir_b: str, action: str):
    config = DuplicatesCleanup(TEST_CONFIG)
    assert config.select_dir_to_keep(dir_a, dir_b) == action
