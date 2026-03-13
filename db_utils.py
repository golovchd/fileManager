from __future__ import annotations

import logging
import random
import sqlite3
from pathlib import Path
from time import sleep
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
DB_SCHEMA_DIR = SCRIPT_DIR / "db_schema"
DB_SCHEMA = [
    DB_SCHEMA_DIR / "fileManager_schema.sql",
]

TABLE_SELECT = "SELECT `ROWID`, `{}`.* FROM `{}` ORDER BY `ROWID`"
# Tables and indexes to ignore
TABLE_COMPARE: dict[str, list[int]] = {
    "types": [],
    "files": [3],
    "disks": [1, 2],
    "fsrecords": [4, 7],
}

_RETRY_COUNT = 10
_RETRY_FIRST_DELAY = 1
_RETRY_RANDOM_DELAY = 0.2   # Ensure spread of retries for multi thread scenarios
_RETRY_DELAY_EXP = 1.5


def create_db(db_path: Path, db_dumps: list[Path]) -> None:
    connection = sqlite3.connect(db_path)
    for db_dump in db_dumps:
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


def initialize_database(db_path: Path):
    """Initialize database."""
    if db_path.exists():
        return
    logging.info(f"Creating database at {db_path}")
    if not db_path.parent.exists():
        logging.info(f"Creating parent directory for database at {db_path.parent}")
        db_path.parent.mkdir(parents=True, exist_ok=True)
    create_db(db_path, DB_SCHEMA)


class SQLite3connection:
    """Basic sqlite3 coonection functionality."""

    def __init__(self, db_path: Path):
        self._db_path = db_path
        initialize_database(db_path)
        self._con = sqlite3.connect(db_path, timeout=10)
        self.SQLITE_LIMIT_VARIABLE_NUMBER = max(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 32766) if hasattr(sqlite3, 'SQLITE_LIMIT_VARIABLE_NUMBER') else 32766
        logging.info(f"Using DB {db_path}")

    def __enter__(self):
        """Allows use with and to ensure connection closure on exit."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closing connect to DB."""
        del exc_type, exc_value, traceback
        if self._con:
            self._con.close()

    def get_connection(self) -> sqlite3.Connection:
        """Returns the sqlite3 connection."""
        return sqlite3.connect(self._db_path, timeout=10)

    def _exec_query(self, sql: str, params: Sequence, commit: bool=True, connection: sqlite3.Connection | None = None):
        """SQL quesy executor with logging."""
        delay: float = _RETRY_FIRST_DELAY
        total_delay: float = 0
        working_con = connection if connection else self._con

        for retry in range(_RETRY_COUNT):
            try:
                result = working_con.execute(sql, params)
                if commit:
                    working_con.commit()
                logging.debug("SQL succeed: %s with %r", sql, params)
                if retry > _RETRY_COUNT // 2:
                    logging.warning(
                        "SQL succeed after %d retries and total delay %f.2 sec: %s with %r", retry, total_delay, sql, params)
                return result
            except sqlite3.OperationalError as e:
                if (str(e) == "database is locked"):
                    logging.debug(
                        "SQL failed as %s, retry %d after %ds: %s with %r",
                        e, retry, delay, sql, params)
                    if retry > _RETRY_COUNT // 2:
                        logging.warning(
                            "DB locked after %d retries and total delay %f.2 sec: %s with %r", retry, total_delay, sql, params)
                    if not retry:   # Add random delay for first retry to ensure spread of retries for multi thread scenarios
                        delay += random.random() * _RETRY_RANDOM_DELAY
                    sleep(delay)
                    total_delay += delay
                    delay *= _RETRY_DELAY_EXP
                    continue
                logging.exception(
                    "SQL failed as %s: %s with %r", e, sql, params)
                raise e
        else:
            logging.error(
                "SQL failed after %d retries: %s with %r", _RETRY_COUNT, sql, params)
            raise sqlite3.OperationalError(
                f"SQL failed after {_RETRY_COUNT} retries: {sql} with {params}")
