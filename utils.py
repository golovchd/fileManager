from datetime import datetime


def float2timestamp(float_timestamp: float) -> datetime:
    """Converts POSIX timestamp to datetime.datetime object."""
    return datetime.fromtimestamp(float_timestamp)


def timeobj2exif_str(timestamp_obj: datetime) -> str:
    """Converts datetime.datetime object to exif_time."""
    return timestamp_obj.strftime("%Y:%m:%d %H:%M:%S")


def timestamp2exif_str(float_timestamp: float) -> str:
    """Converts POSIX timestamp to exif_time."""
    return timeobj2exif_str(float2timestamp(float_timestamp))
