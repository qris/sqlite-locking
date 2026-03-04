"""Tests for sqlite_locking.sqlite_extension."""

import logging
import os
import sqlite3

import pytest
from sqlite_locking.enums import SqliteDatabaseStatus
from sqlite_locking.extension import load_extension

logger = logging.getLogger(__file__)

@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database without opening it."""
    return os.path.join(tmp_path, 'database.tmp')

@pytest.fixture
def db_extension(db_path):
    """Test fixture that yields a sqlite3.Connection with sqlite_extension loaded."""
    with sqlite3.connect(db_path) as db:
        load_extension(db, 'sqlite_extension')
        yield db

def test_query_cache_spills(db_extension, db_path):
    """
    Test that our sqlite3_db_status wrapper can be used to count cache spills.

    This is based on cachespill.test in the SQLite source.
    """
    db = db_extension
    db.execute("PRAGMA auto_vacuum = 0").close()
    # db.execute("PRAGMA page_size = 1024").close()  # doesn't change page_size
    db.execute("PRAGMA cache_size = -100").close()  # limit to 100k
    db.execute("CREATE TABLE t1(a)").close()
    db.execute("BEGIN").close()
    db.execute("""
    WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<200
    ) INSERT INTO t1 SELECT randomblob(900) FROM s;
    """).close()
    result = SqliteDatabaseStatus.CACHE_SPILL.get(db)
    # first value is result code (0 = SQLITE_OK), second value is number of
    # cache spills, which could vary with SQLite version:
    assert result == 31
