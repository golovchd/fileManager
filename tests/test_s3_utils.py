from __future__ import annotations

import pytest

from file_manager.s3_utils import S3Client


def test_is_symlink() -> None:
    assert not S3Client("s3://bucket").is_symlink("foo")


@pytest.mark.parametrize(
    "bucket_path, name",
    [
        ("s3://bucket/", "bucket"),
        ("s3://bucket", "bucket"),
        ("s3://bucket/dir/", "bucket"),
        ("s3://bucket/dir", "bucket"),
    ]
)
def test_disk_name(bucket_path: str, name: str) -> None:
    assert S3Client(bucket_path).disk_name == name


@pytest.mark.parametrize(
    "bucket_path, path_from_mount",
    [
        ("s3://bucket/", []),
        ("s3://bucket", []),
        ("s3://bucket//", [""]),
        ("s3://bucket/dir/", ["dir"]),
        ("s3://bucket/dir", ["dir"]),
        ("s3://bucket/dir/", ["dir"]),
        ("s3://bucket/dir//", ["dir", ""]),
        ("s3://bucket//dir/", ["", "dir"]),
    ]
)
def test_path_from_mount(bucket_path: str, path_from_mount: list[str]) -> None:
    assert S3Client(bucket_path).get_path_from_mount() == path_from_mount


@pytest.mark.parametrize(
    "bucket_path, media",
    [
        ("s3://bucket/", "bucket/"),
        ("s3://bucket//", "bucket//"),
        ("s3://bucket", "bucket/"),
        ("s3://bucket/dir/", "bucket/dir/"),
        ("s3://bucket/dir", "bucket/dir/"),
        ("s3://bucket/dir//", "bucket/dir//"),
        ("s3://bucket//dir/", "bucket//dir/"),
        ("s3://bucket//dir", "bucket//dir/"),
    ]
)
def test_mount(bucket_path: str, media: str) -> None:
    assert S3Client(bucket_path).media == media
