from __future__ import annotations

from pathlib import Path

import pytest

from file_manager.import_config import ImportConfig
from file_manager.import_media import _DEFAULT_CONFIG

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"


@pytest.mark.parametrize(
    "test_config",
    [
        (_DEFAULT_CONFIG),
        (TEST_DATA_DIR / "min_import_config.yaml"),
    ]
)
def test_default_config(test_config: Path):
    config = ImportConfig(test_config)
    assert config.storage_regex_list is not None
    assert config.import_roots_list is not None
    assert config.free_space_limits is not None
    assert config.media_config is not None
