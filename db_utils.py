import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

TABLE_SELECT = "SELECT `ROWID`, `{}`.* FROM `{}` ORDER BY `ROWID`"
# Tables and indexes to ignore
TABLE_COMPARE: Dict[str, List[int]] = {
    "types": [],
    "files": [3],
    "disks": [1, 2],
    "fsrecords": [4, 7],
}


def create_db(db_path: Path, db_dump: Path) -> None:
    connection = sqlite3.connect(db_path)
    connection.executescript(db_dump.read_text())
    connection.close()


def dump_db(db_path: Path, db_dump: Path):
    connection = sqlite3.connect(db_path)
    with open(db_dump, 'w') as f:
        for line in connection.iterdump():
            f.write('%s\n' % line)
    connection.close()


def compare_db_with_ignores(db1_path: Path, dg2_path: Path) -> None:
    connection_1 = sqlite3.connect(db1_path)
    connection_2 = sqlite3.connect(dg2_path)
    for table, excludes in TABLE_COMPARE.items():
        res_1 = connection_1.execute(TABLE_SELECT.format(table, table))
        res_2 = connection_2.execute(TABLE_SELECT.format(table, table))
        while True:
            rows_1 = res_1.fetchone()
            rows_2 = res_2.fetchone()
            if rows_1 is None and rows_2 is None:
                break
            if rows_1 is None or rows_2 is None:
                assert 0
            for i in range(len(rows_1)):
                if i not in excludes:
                    assert rows_1[i] == rows_2[i]
    connection_1.close()
    connection_2.close()


class SQLite3connection:
    """Basic sqlite3 coonection functionality."""

    def __init__(self, db_path: Path):
        self._con = sqlite3.connect(db_path, timeout=10)
        logging.info(f"Using DB {db_path}")

    def __enter__(self):
        """Allows use with and to ensure connection closure on exit."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closing connect to DB."""
        del exc_type, exc_value, traceback
        if self._con:
            self._con.close()

    def _exec_query(self, sql: str, params: Tuple, commit=True):
        """SQL quesy executor with logging."""
        try:
            result = self._con.execute(sql, params)
            if commit:
                self._con.commit()
            logging.debug("SQL succeed: %s with %r", sql, params)
            return result
        except sqlite3.OperationalError as e:
            logging.exception("SQL failed: %s with %r", sql, params)
            raise e
