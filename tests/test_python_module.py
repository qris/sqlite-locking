"""Tests for sqlite_locking.sqlite_extension."""

import logging
import os
import sqlite3

import pytest
from sqlite_locking.enum import SqliteDatabaseStatus, SqliteDBConfig
from sqlite_locking.extension import load_extension
from sqlite_locking.python_module import (
    SQLITE_IOERR_DELETE,
    SQLITE_IOERR_DELETE_NOENT,
    SQLITE_MISMATCH,
    SQLITE_NOTFOUND,
    SQLITE_OK,
    nodeletefs_delete,
    nodeletefs_init,
    sqlite3_db_config,
    sqlite3_errorlog_init,
    sqlite3_errorlog_read_logs,
    sqlite3_log,
    sqlite3_set_persist_wal,
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


def test_nodeletefs(db_path, tmp_path):
    """Test that our nodeletefs interface works."""
    # Unregister nonexistent vfs:
    assert nodeletefs_init("nodeletefs_test", "", "") == SQLITE_NOTFOUND

    # Unregister a different vfs:
    assert nodeletefs_init("unix", "", "") == SQLITE_MISMATCH

    # Initialise nodeletefs_test and vfstrace (so we can see what it did).
    # Prevent deletion of a journal file:
    assert nodeletefs_init("nodeletefs_test", sqlite3_vfs_default(), f"{db_path}-journal") == SQLITE_OK
    assert sqlite3_vfstrace_init("vfstrace_test", "nodeletefs_test") == 0

    with sqlite3.connect(f"file://{db_path}?vfs=vfstrace_test", uri=True) as db:
        assert db.execute("PRAGMA vfstrace('-all, +Delete');").fetchall() == []
        sqlite3_vfstrace_read_logs()  # clear and discard logs from opening DB
        # Switching to WAL mode should fail because we can't delete the old
        # journal file:
        with pytest.raises(sqlite3.OperationalError):
            db.execute("PRAGMA journal_mode=WAL").fetchall()
        assert sqlite3_vfstrace_read_logs() == [
            f'vfstrace_test.xDelete("{db_path}-journal",0)',
            " -> SQLITE_IOERR_DELETE\n",
        ]

    # Initialise nodeletefs_test again, this time preventing deletion of a
    # random file not actually used by SQLite:
    nodelete_filename = f"{db_path}-nodelete"
    assert nodeletefs_init("nodeletefs_test", sqlite3_vfs_default(), nodelete_filename) == SQLITE_OK

    with sqlite3.connect(f"file://{db_path}?vfs=vfstrace_test", uri=True) as db:
        assert db.execute("PRAGMA vfstrace('-all, +Delete');").fetchall() == []
        # Switching to WAL mode should work this time:
        db.execute("PRAGMA journal_mode=WAL").fetchall()
        sqlite3_vfstrace_read_logs()  # clear and discard logs from opening DB

        # Trigger deletion of a file:
        file_to_delete = str(tmp_path / "nonexistent.foo")
        assert nodeletefs_delete(db, "main", file_to_delete) == SQLITE_IOERR_DELETE_NOENT
        assert sqlite3_vfstrace_read_logs() == [
            f'vfstrace_test.xDelete("{file_to_delete}",0)',
            " -> SQLITE_IOERR | 0x1700\n",
        ]

        # Check that we can delete a file that exists, which is not nodelete_filename:
        with open(file_to_delete, "x"):
            pass
        assert nodeletefs_delete(db, "main", file_to_delete) == SQLITE_OK
        assert sqlite3_vfstrace_read_logs() == [
            f'vfstrace_test.xDelete("{file_to_delete}",0)',
            " -> SQLITE_OK\n",
        ]

        # Check that we can't delete nodelete_filename:
        with open(nodelete_filename, "x"):
            pass
        assert nodeletefs_delete(db, "main", nodelete_filename) == SQLITE_IOERR_DELETE
        assert sqlite3_vfstrace_read_logs() == [
            f'vfstrace_test.xDelete("{nodelete_filename}",0)',
            " -> SQLITE_IOERR_DELETE\n",
        ]

    # Unregister a valid nodeletefs vfs:
    assert nodeletefs_init("nodeletefs_test", "", "") == SQLITE_OK


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

        # Unfortunately we cannot check for the database being None or closed,
        # but we can at least check that these do not crash:
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(None, 42, -1)
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(None, 42, -1)
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(db, 42, -1)
        with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
            sqlite3_db_config(db, 42, 0)

    db.close()
    with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
        sqlite3_db_config(db, 42, -1)
    with pytest.raises(ValueError, match="Unknown or unsupported config 'op'"):
        sqlite3_db_config(db, 42, 0)


def test_sqlite3_set_persist_wal(db_path):
    """Test that our sqlite3_set_persist_wal function works."""
    # By default, the WAL file should be deleted when the database is closed:
    with sqlite3.connect(f"file://{db_path}", uri=True) as db:
        db.execute("PRAGMA journal_mode=WAL").fetchall()
        db.execute("CREATE TABLE t1(a)").close()
    db.close()
    assert not os.path.exists(f"{db_path}-wal")

    # But not if we set the persist_wal flag first:
    assert sqlite3_vfstrace_init("vfstrace_test", "unix") == 0
    with sqlite3.connect(f"file://{db_path}?vfs=vfstrace_test", uri=True) as db:
        assert sqlite3_set_persist_wal(db, "main", True) == SQLITE_OK
        db.execute("SELECT * FROM t1").close()
    db.close()
    assert os.path.exists(f"{db_path}-wal")

    # And if we clear the flag, the WAL file should be deleted on close again:
    with sqlite3.connect(f"file://{db_path}", uri=True) as db:
        assert sqlite3_set_persist_wal(db, "main", False) == SQLITE_OK
        db.execute("SELECT * FROM t1").close()
    db.close()
    assert not os.path.exists(f"{db_path}-wal")
