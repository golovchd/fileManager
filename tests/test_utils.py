from __future__ import annotations

from typing import Callable
from unittest.mock import call, patch

import pytest

from file_manager.utils import print_table, timestamp2exif_str


@pytest.mark.parametrize(
    "data, headers, indexes, formats, aligns, separator, space, printout",
    [
        (
            [
                [1, "DE7F-59F0", "DG-2TB-SSD1", 1953450496],
                [2, "417c59e1-0328-4a93-bf42-e72782969d72",
                 "DG-5TB-5", 4583134208],
                [6, "cb268b0c-d887-47f8-bcda-35f5f290776f",
                 "5TB-2-P5-364G", 349230960],
            ],
            ["DiskID", "UUID", "Label", "DiskSize"],
            None,
            None,
            None,
            "|",
            " ",
            [
                call("| DiskID |                                 UUID "
                     "|         Label |   DiskSize |"),
                call("|      1 |                            DE7F-59F0 "
                     "|   DG-2TB-SSD1 | 1953450496 |"),
                call("|      2 | 417c59e1-0328-4a93-bf42-e72782969d72 "
                     "|      DG-5TB-5 | 4583134208 |"),
                call("|      6 | cb268b0c-d887-47f8-bcda-35f5f290776f "
                     "| 5TB-2-P5-364G |  349230960 |")
            ]
        ),
        (
            [
                [854367, "Audio", None, None, None, None, None],
                [854507, "Backup", None, None, None, None, None],
                [2258266, "Books", None, None, None, None, None],
            ],
            ["Name", "Size", "File Date", "Hash Date", "SHA256"],
            [1, 5, 2, 3, 6],
            [
                str,
                lambda x: str(x) if x else "dir",
                timestamp2exif_str,
                timestamp2exif_str,
                lambda x: str(x) if x else ""
            ],
            ["<", ">", ">", ">", "<"],
            "|",
            " ",
            [
                call("| Name   | Size | File Date | Hash Date | SHA256 |"),
                call("| Audio  |  dir |           |           |        |"),
                call("| Backup |  dir |           |           |        |"),
                call("| Books  |  dir |           |           |        |"),
            ]
        ),
    ]
)
@patch('builtins.print')
def test_print_table(
        mock_print,
        data: list[list[str]],
        headers: list[str],
        indexes: list[int] | None,
        formats: list[Callable] | None,
        aligns: list[str] | None,
        separator: str,
        space: str,
        printout: list[object]
        ) -> None:
    print_table(data, headers, indexes=indexes, formats=formats,
                aligns=aligns, separator=separator, space=space)
    assert mock_print.call_args_list == printout
