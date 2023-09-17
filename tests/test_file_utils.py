import sys
from hashlib import sha1
from pathlib import Path
from typing import List

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR.parent))

from file_utils import (generate_file_sha1, get_full_dir_path,  # noqa: E402
                        get_mount_path, get_path_from_mount, read_dir)


def test_get_full_dir_path():
    assert get_full_dir_path(Path("~")) == Path.home()
    assert get_full_dir_path(SCRIPT_DIR / "../test_data") == (
        SCRIPT_DIR.parent / "test_data")
    assert get_full_dir_path(Path(__file__)) == SCRIPT_DIR


def test_get_mount_path():
    assert get_mount_path(SCRIPT_DIR).is_mount()


@pytest.mark.parametrize(
    "test_dir_path, mount_path, expected_result",
    [
        (
            Path("/media/disk/DCIM/DCIM0001"),
            Path("/media/disk"),
            ["DCIM", "DCIM0001"]
        ),
        (
            Path("/media/disk/A/B/C/D/E/F"),
            Path("/media/disk/"),
            ["A", "B", "C", "D", "E", "F"]
        ),
        (
            Path("/media/disk/A/B/C/D/E/F/"),
            Path("/media/disk/"),
            ["A", "B", "C", "D", "E", "F"]
        ),
        (
            Path("/media/disk/A/B/C/D/E/F/"),
            Path("/media/disk"),
            ["A", "B", "C", "D", "E", "F"]
        ),
        (
            Path("/home/user/"),
            Path("/"),
            ["home", "user"]
        ),
        ],
)
def test_get_path_from_mount(
        mocker, test_dir_path, mount_path, expected_result):
    def mock_get_mount_path(dir_path: Path) -> List[str]:
        del dir_path
        return mount_path

    mocker.patch("file_utils.get_mount_path", mock_get_mount_path)
    assert get_path_from_mount(test_dir_path) == expected_result


def test_get_path_from_mount_real_path():
    path_list = get_path_from_mount(SCRIPT_DIR)
    test_path = get_mount_path(SCRIPT_DIR)
    for dir in path_list:
        test_path /= dir
    assert test_path == SCRIPT_DIR


@pytest.mark.parametrize(
    "test_path, expected_files, expected_dirs",
    [
        ("test_data", [], ["media", "storage"]),
        ("test_data/media",
         [
             "6TB-2 benchmark 2018-08-25 20-58-29.png",
             "DSC06979.JPG",
             "IMG_0004.JPG",
             "IMG_0013.JPG",
             "not_an_image"
         ],
         []),
        ("test_data/storage",
         [
             "DSC06979c.JPG",
             "DSC06979 (copy).JPG",
             "DSC06979.JPG"
         ],
         ["tagged"]),
        ("test_data/storage/tagged", ["DSC06979.JPG", "IMG_0004.JPG"], []),
    ],
)
def test_read_dir(test_path, expected_files, expected_dirs):
    files, sub_dirs = read_dir(SCRIPT_DIR.parent / test_path)
    assert sorted(files) == sorted(expected_files)
    assert sorted(sub_dirs) == sorted(expected_dirs)


def test_generate_file_sha1():
    for file_path in (SCRIPT_DIR.parent / "test_data").glob("**/*"):
        print(f"Testing {file_path}")
        if file_path.is_dir():
            continue
        sha1_hash = sha1()
        sha1_hash.update(file_path.read_bytes())
        assert generate_file_sha1(file_path, 1024) == sha1_hash.hexdigest()
