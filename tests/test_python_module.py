"""Tests for sqlite_locking.sqlite_extension."""

import logging
import os
import sqlite3

import pytest
from sqlite_locking.enum import SqliteDatabaseStatus, SqliteDBConfig
from sqlite_locking.extension import load_extension
from sqlite_locking.python_module import (
    sqlite3_db_config,
    sqlite3_errorlog_init,
    sqlite3_errorlog_read_logs,
    sqlite3_log,
    sqlite3_vfs_default,
    sqlite3_vfstrace_init,
    sqlite3_vfstrace_read_logs,
)

logger = logging.getLogger(__file__)


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database without opening it."""
    return os.path.join(tmp_path, "database.tmp")


@pytest.fixture
def db(db_path):
    """Test fixture that yields a temporary sqlite3.Connection."""
    with sqlite3.connect(db_path) as db:
        yield db


@pytest.fixture
def db_extension(db_path):
    """Test fixture that yields a sqlite3.Connection with sqlite_extension loaded."""
    with sqlite3.connect(db_path) as db:
        load_extension(db, "sqlite_extension")
        yield db


def test_query_cache_spills(db):
    """
    Test that our sqlite3_db_status wrapper can be used to count cache spills.

    This is based on cachespill.test in the SQLite source.
    """
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


def test_errorlog(db):
    """
    Test that our sqlite error log wrapper works.

    This is a Python interface to the SQLite "error logging callback". See
    <https://sqlite.org/errlog.html> for details.
    """
    sqlite3_errorlog_init()
    assert sqlite3_errorlog_read_logs() == []

    sqlite3_log(42, "hello world")
    assert sqlite3_errorlog_read_logs() == [(42, "hello world")]

    with pytest.raises(sqlite3.OperationalError):
        db.execute("INVALID SQL;")
    assert sqlite3_errorlog_read_logs() == [(1, 'near "INVALID": syntax error in "INVALID SQL;"')]


def test_vfstrace(db_path):
    """Test that our vfstrace interface works."""
    assert sqlite3_vfstrace_init("vfstrace_test", sqlite3_vfs_default()) == 0
    with sqlite3.connect(f"file://{db_path}:?vfs=vfstrace_test", uri=True) as db:
        sqlite3_vfstrace_read_logs()  # clear and discard logs from opening DB
        assert db.execute("PRAGMA vfstrace('-all, +Lock,Unlock,Truncate,FileControl');").fetchall() == []
        assert db.execute("PRAGMA journal_mode=WAL").fetchall() == [("wal",)]
        assert sqlite3_vfstrace_read_logs() == [
            "vfstrace_test.xFileControl(database.tmp:,PRAGMA,[vfstrace,-all, +Lock,Unlock,Truncate,FileControl])",
            " -> SQLITE_NOTFOUND\n",
            "vfstrace_test.xFileControl(database.tmp:,PRAGMA,[journal_mode,WAL])",
            " -> SQLITE_NOTFOUND\n",
            "vfstrace_test.xLock(database.tmp:,SHARED)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xUnlock(database.tmp:,NONE)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xLock(database.tmp:,SHARED)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xLock(database.tmp:,RESERVED)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xLock(database.tmp:,EXCLUSIVE)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xFileControl(database.tmp:,SIZE_HINT,4096)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xFileControl(database.tmp:,SYNC)",
            " -> SQLITE_NOTFOUND\n",
            "vfstrace_test.xFileControl(database.tmp:,COMMIT_PHASETWO)",
            " -> SQLITE_NOTFOUND\n",
            "vfstrace_test.xUnlock(database.tmp:,SHARED)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xUnlock(database.tmp:,NONE)",
            " -> SQLITE_OK\n",
        ]

        db.execute("BEGIN IMMEDIATE").close()
        assert sqlite3_vfstrace_read_logs() == [
            "vfstrace_test.xLock(database.tmp:,SHARED)",
            " -> SQLITE_OK\n",
            "vfstrace_test.xFileControl(database.tmp:,MMAP_SIZE,0)",
            " -> SQLITE_OK",
            ", 0\n",
        ]

        db.execute("CREATE TABLE t (ID integer)").close()
        assert sqlite3_vfstrace_read_logs() == [
            "vfstrace_test.xFileControl(database.tmp:,PRAGMA,[integrity_check,t])",
            " -> SQLITE_NOTFOUND\n",
        ]

        db.execute("COMMIT").close()
        assert sqlite3_vfstrace_read_logs() == [
            "vfstrace_test.xFileControl(database.tmp:,COMMIT_PHASETWO)",
            " -> SQLITE_NOTFOUND\n",
        ]


def test_sqlite3_db_config():
    """Test that our sqlite3_db_config wrapper works."""
    with sqlite3.connect(":memory:") as db:
        # Initial value should be False:
        assert not SqliteDBConfig.NO_CKPT_ON_CLOSE.get(db)
        # We should be able to set it to True:
        assert SqliteDBConfig.NO_CKPT_ON_CLOSE.set(db, True)
        assert SqliteDBConfig.NO_CKPT_ON_CLOSE.get(db)
        # And back to False:
        assert not SqliteDBConfig.NO_CKPT_ON_CLOSE.set(db, False)
        assert not SqliteDBConfig.NO_CKPT_ON_CLOSE.get(db)

        with pytest.raises(ValueError, match="Invalid connection or base Connection.__init__ not called"):
            sqlite3_db_config(None, 42, -1)
        with pytest.raises(ValueError, match="Invalid connection or base Connection.__init__ not called"):
            sqlite3_db_config(None, 42, -1)
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(db, 42, -1)
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(db, 42, 0)

    db.close()
    with pytest.raises(ValueError, match="Cannot operate on a closed database"):
        sqlite3_db_config(db, 42, -1)
    with pytest.raises(ValueError, match="Cannot operate on a closed database"):
        sqlite3_db_config(db, 42, 0)
