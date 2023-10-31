from datetime import datetime
from typing import Callable, List, Optional


def float2timestamp(float_timestamp: float) -> datetime:
    """Converts POSIX timestamp to datetime.datetime object."""
    return datetime.fromtimestamp(float_timestamp)


def timeobj2exif_str(timestamp_obj: datetime) -> str:
    """Converts datetime.datetime object to exif_time."""
    return timestamp_obj.strftime("%Y-%m-%d %H:%M:%S")


def timestamp2exif_str(float_timestamp: float) -> str:
    """Converts POSIX timestamp to exif_time."""
    return (timeobj2exif_str(float2timestamp(float_timestamp))
            if float_timestamp else "")


def print_table(
        data: List[List[str]],
        headers: List[str],
        indexes: Optional[List[int]] = None,
        formats: Optional[List[Callable]] = None,
        aligns: Optional[List[str]] = None,
        separator: str = "|",
        space: str = " ",
        ) -> None:
    column_count = len(headers)
    column_sizes = [len(header) for header in headers]
    if not indexes:
        indexes = list(range(column_count))
    if not formats:
        formats = [str for _ in range(column_count)]
    if not aligns:
        aligns = [">" for _ in range(column_count)]
    for row in data:
        for i in range(column_count):
            column_sizes[i] = max(
                column_sizes[i], len(formats[i](row[indexes[i]])))
    format_str = separator
    for i in range(column_count):
        format_str += (f"{space}{{:{aligns[i]}{column_sizes[i]}}}"
                       f"{space}{separator}")
    print(format_str.format(*headers))
    for row in data:
        print_data = [formats[i](row[indexes[i]])
                      for i in range(column_count)]
        print(format_str.format(*print_data))
