from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import (SQLite3connection, compare_db_with_ignores, create_db,
                      dump_db)

_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "test_db/fileManager_test_dump.sql"
_TEST_DB_NAME = "test.db"


def test_lazy_db_setup(tmp_path: Path):
    assert not (tmp_path / _TEST_DB_NAME).exists()
    SQLite3connection(tmp_path / _TEST_DB_NAME)
    assert (tmp_path / _TEST_DB_NAME).exists()

def test_create_db(tmp_path: Path):
    create_db_path = tmp_path / _TEST_DB_NAME
    create_db(create_db_path, _DB_TEST_DB_DUMP)
    assert create_db_path.exists()
    with SQLite3connection(create_db_path) as db:
        tables = {row[0] for row in db._exec_query("SELECT name FROM sqlite_master WHERE type='table'", [], commit=False)}
        expected_tables = {"disks", "fsrecords", "files", "types"}
        assert tables == expected_tables

def test_create_compare_db(tmp_path: Path):
    create_db_path1 = tmp_path / _TEST_DB_NAME
    create_db_path2 = tmp_path / "test2.db"
    create_db(create_db_path1, _DB_TEST_DB_DUMP)
    create_db(create_db_path2, _DB_TEST_DB_DUMP)
    compare_db_with_ignores(create_db_path1, create_db_path2)

def test_dump_db(tmp_path: Path):
    create_db_path = tmp_path / _TEST_DB_NAME
    dump_db_path = tmp_path / "dump.sql"
    create_db(create_db_path, _DB_TEST_DB_DUMP)
    dump_db(create_db_path, dump_db_path)
    assert dump_db_path.exists()
    with open(dump_db_path) as f:
        dump_content = f.read()
    assert "CREATE TABLE \"disks\"" in dump_content
    assert "CREATE TABLE \"fsrecords\"" in dump_content
    assert "CREATE TABLE \"files\"" in dump_content
    assert "CREATE TABLE \"types\"" in dump_content
    assert "INSERT INTO \"fsrecords\" VALUES('fileManager',4,1,NULL,NULL,NULL,NULL);" in dump_content
