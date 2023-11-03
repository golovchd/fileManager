import sys
from hashlib import sha1
from pathlib import Path
from typing import List

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from file_utils import (generate_file_sha1, get_full_dir_path,  # noqa: E402
                        get_mount_path, get_path_disk_info,
                        get_path_from_mount, read_dir, read_file)


def test_get_full_dir_path():
    assert get_full_dir_path(Path("~")) == Path.home()
    assert get_full_dir_path(SCRIPT_DIR / "../test_data") == TEST_DATA_DIR
    assert get_full_dir_path(Path(__file__)) == SCRIPT_DIR


def test_get_mount_path():
    assert get_mount_path(SCRIPT_DIR).is_mount()


@pytest.mark.parametrize(
    "test_dir_path, mount_path, expected_result",
    [
        (
            Path("/media/disk"),
            Path("/media/disk"),
            [""]
        ),
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
             "not.an.image.txt",
             "not_an_image"
         ],
         []),
        ("test_data/storage",
         [
             "DSC06979c.JPG",
             "DSC06979 (copy).JPG",
             "DSC06979.JPG"
         ],
         ["second_dir", "tagged"]),
        ("test_data/storage/tagged", ["DSC06979.JPG", "IMG_0004.JPG"], []),
        ("test_data/storage/second_dir", ["foo.txt"], []),
    ],
)
def test_read_dir(test_path, expected_files, expected_dirs):
    files, sub_dirs = read_dir(SCRIPT_DIR.parent / test_path)
    assert sorted(files) == sorted(expected_files)
    assert sorted(sub_dirs) == sorted(expected_dirs)


def test_generate_file_sha1():
    for file_path in TEST_DATA_DIR.glob("**/*"):
        print(f"Testing {file_path}")
        if file_path.is_dir():
            continue
        sha1_hash = sha1()
        sha1_hash.update(file_path.read_bytes())
        file_sha, _ = generate_file_sha1(file_path, 1024)
        assert file_sha == sha1_hash.hexdigest()


@pytest.mark.parametrize(
    "file_path, file_name, file_type, size, sha1_hex",
    [
        (
            "media/6TB-2 benchmark 2018-08-25 20-58-29.png",
            "6TB-2 benchmark 2018-08-25 20-58-29",
            "png",
            239027,
            "db85197ec899df5cbd2a7fb28bf30a1b9875f2ed"
        ),
        (
            "media/not.an.image.txt",
            "not.an.image",
            "txt",
            16,
            "c19dfe09be521ccdf6957794128aef97c592baf6"
        ),
        (
            "media/not_an_image",
            "not_an_image",
            "",
            18,
            "956f4ca7fd5877604544213c6b66b33416ebfb3f"
        ),
    ],
)
def test_read_file(
        file_path: str,
        file_name: str,
        file_type: str,
        size: int,
        sha1_hex: str
        ) -> None:
    (
        read_file_name, read_file_type, read_size, _, read_sha1, _
    ) = read_file(TEST_DATA_DIR / file_path, True)
    assert read_file_name == file_name
    assert read_file_type == file_type
    assert read_size == size
    assert read_sha1 == sha1_hex
    (
        read_file_name, read_file_type, read_size, _, read_sha1, _
    ) = read_file(TEST_DATA_DIR / file_path, False)
    assert read_file_name == file_name
    assert read_file_type == file_type
    assert read_size == size
    assert read_sha1 == ''


@pytest.mark.parametrize(
    "dir_path, df_info, lsbls_info, uuid, label, size",
    [
        (
            "/media/user/My Backup 8/data",
            b"\n".join([
                b"Filesystem      1K-blocks",
                b"/dev/sde1      1953513556",
                b""
            ]),
            b"\n".join([
                b"UUID                                 LABEL",
                b"F2FC151BAC04DF13                     My Backup 8",
                b""
            ]),
            "F2FC151BAC04DF13",
            "My Backup 8",
            1953513556,
        ),
        (
            "/media/user/My Backup 8/data",
            b"\n".join([
                b"Filesystem      1K-blocks",
                b"/dev/sde1      1953513556",
                b""
            ]),
            b"\n".join([
                b"UUID                                 LABEL",
                b"                                     My Backup 8",
                b""
            ]),
            "",
            "My Backup 8",
            1953513556,
        ),
        (
            "/media/user/My Backup 8/data",
            b"\n".join([
                b"Filesystem      1K-blocks",
                b"/dev/sde1      1953513556",
                b""
            ]),
            b"\n".join([
                b"UUID                                 LABEL",
                b"F2FC151BAC04DF13                     My Backup 8",
                b""
            ]),
            "F2FC151BAC04DF13",
            "My Backup 8",
            1953513556,
        ),
        (
            "/media/user/DISK_LABEL/data",
            b"\n".join([
                b"Filesystem      1K-blocks",
                b"/dev/sde1      1953513556",
                b""
            ]),
            b"\n".join([
                b"UUID                                 LABEL",
                b"F2FC151BAC04DF13                     DISK_LABEL",
                b""
            ]),
            "F2FC151BAC04DF13",
            "DISK_LABEL",
            1953513556,
        ),
    ],
)
def test_get_path_disk_info(
        mocker,
        dir_path: str,
        df_info: bytes,
        lsbls_info: bytes,
        uuid: str,
        label: str,
        size: int
        ) -> None:

    mocker.patch("subprocess.check_output",
                 lambda cmd: df_info if cmd[0] == "df" else lsbls_info)
    disk_info = get_path_disk_info(dir_path)
    assert disk_info["uuid"] == uuid
    assert disk_info["label"] == label
    assert disk_info["size"] == size
