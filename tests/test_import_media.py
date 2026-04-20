"""Import Media module unittests."""
from __future__ import annotations

import re
from datetime import MINYEAR, datetime
from pathlib import Path

import pytest

from file_manager.import_config import MediaConfig  # type: ignore
from file_manager.import_media import ExifTimeError  # type: ignore
from file_manager.import_media import (exif_time2unix, file_type_from_name,
                                       get_import_list, get_media_list,
                                       read_file_time)

TEST_SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = TEST_SCRIPT_DIR.parent / "test_data"


@pytest.mark.parametrize(
    "file_path, type",
    [
        (Path('test.JPG'), 'jpg'),
        (Path('test.mpg'), 'mpg'),
        (Path('dir/test.mpg'), 'mpg'),
        (Path('dir/test.rx'), 'rx'),
        (Path('dir/test'), ''),
        (Path('/dir/path/'), ''),
    ]
)
def test_file_type(file_path: Path, type: str) -> None:
    assert file_type_from_name(file_path) == type


def test_exif_time2unix() -> None:
    with pytest.raises(ExifTimeError):
        exif_time2unix('2018:06:30')
    with pytest.raises(ExifTimeError):
        test_file_date = read_file_time(
            TEST_DATA_DIR / 'media/not_an_image', True)
    assert exif_time2unix('0000:00:00 00:00:00') == datetime(MINYEAR, 1, 1)
    assert exif_time2unix('2019-09-04T05:00:00') == datetime(2019, 9, 4, 5, 0, 0)
    test_file_date = read_file_time(
        TEST_DATA_DIR / 'media/DSC06979.JPG', True)
    assert test_file_date == datetime(2018, 2, 19, 11, 5, 43)


@pytest.mark.parametrize(
    "only_same_names",
    [
        (True),
        (False),
    ]
)
def test_get_import_list(only_same_names: bool) -> None:
    missing_list = [
        '6TB-2 benchmark 2018-08-25 20-58-29.png',
        'IMG_0013.JPG',
    ]
    missing_list_tagged = [
        '6TB-2 benchmark 2018-08-25 20-58-29.png',
        'DSC06979.JPG',
        'IMG_0013.JPG',
    ]
    present_dict = {
        'DSC06979.JPG': TEST_DATA_DIR / 'storage/DSC06979.JPG',
        'IMG_0004.JPG': TEST_DATA_DIR / 'storage/tagged/IMG_0004.JPG',
    }
    present_tagged_dict = {
        'DSC06979.JPG': TEST_DATA_DIR / 'storage/tagged/DSC06979.JPG',
        'IMG_0004.JPG': TEST_DATA_DIR / 'storage/tagged/IMG_0004.JPG',
    }
    not_imported, already_imported = get_import_list(
        TEST_DATA_DIR / 'media', TEST_DATA_DIR / 'storage', [], only_same_names)
    assert sorted(missing_list) == sorted(not_imported)
    assert present_dict == already_imported
    not_imported, already_imported = get_import_list(
        TEST_DATA_DIR / 'media', TEST_DATA_DIR / 'storage/tagged', [], only_same_names)
    assert sorted(missing_list_tagged) == sorted(not_imported)
    assert present_tagged_dict, already_imported


root_empty_config = MediaConfig("root", re.compile("^$"), [])

@pytest.mark.parametrize(
    "media_config, expected_result",
    [
        ({"root": root_empty_config}, {Path("/"): root_empty_config}),
    ]
)
def test_get_media_list(media_config: dict[str, MediaConfig], expected_result: dict[Path, MediaConfig]) -> None:
    assert get_media_list(media_config) == expected_result
