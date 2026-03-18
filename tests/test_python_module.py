"""Tests for sqlite_locking.sqlite_extension."""

import logging
import os
import sqlite3
from enum import Enum, auto

import pytest
from sqlite_locking.enum import SqliteDatabaseStatus, SqliteDBConfig
from sqlite_locking.extension import load_extension
from sqlite_locking.python_module import (
    SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
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


class WalFileDisposition(Enum):
    """Options for what should happen to the WAL file in test_repeated_backfills."""

    NORMAL = auto()  # normal behaviour, WAL file deleted on close
    NODELETE = auto()  # nodeletefs extension prevents removal of WAL file
    PERSIST = auto()  # SQLITE_FCNTL_PERSIST_WAL prevents removal of WAL file
    PERSIST_LIMIT = auto()  # SQLITE_FCNTL_PERSIST_WAL with a journal size limit, truncates WAL instead
    CHECKPOINT = auto()  # explicit wal_checkpoint() before close
    NO_CHECKPOINT = auto()  # SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE prevents checkpoint on close


@pytest.mark.parametrize("disposition", list(WalFileDisposition))
def test_repeated_backfills(disposition, db_path):
    """
    Test the "bug" that causes unexpected repeated backfilling from WAL to DB file.

    "Usually, the WAL file is deleted automatically when the last connection to
    the database closes." However, if this doesn't happen for some reason, for
    example:

    * The WAL file is owned by a different user, so deletion fails with permission denied.
    * SQLITE_FCNTL_PERSIST_WAL is used to prevent its deletion on close.
    * SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE is used to prevent the checkpoint on close,
      which prevents the WAL file from being deleted as it is still in use.

    (corresponding to dispositions NODELETE, PERSIST and NO_CHECKPOINT above)
    then the next connection runs recovery on the leftover WAL file. "Since the
    recovery procedure has no way of knowing how many frames of the WAL might
    have previously been copied back into the database, it initializes the
    nBackfill value to zero."

    So, on the next checkpoint or when the database is closed again, the entire
    WAL will be backfilled into the main database file **again** (starting from
    nBackfill == 0). Note that in cases NODELETE and PERSIST above, the entire
    WAL file has already been backfilled (by the successful checkpoint on
    close), so nBackflll should equal mxFrame, not zero.

    See <https://sqlite.org/forum/forumpost/0897cec5bf> for more details.

    Note that PERSIST_LIMIT (combining `SQLITE_FCNTL_PERSIST_WAL` with
    `PRAGMA journal_size_limit`) seems to be a reasonable workaround, because it
    truncates the WAL file on close (instead of deleting it), so there is
    nothing for the next connection to recover.
    """
    wal_file = f"{db_path}-wal"

    # Do we expect to see recovery run on second connection to the database?
    # That depends on the disposition for this test case. I think this is a bug
    # (should not happen if the database was closed cleanly) but this test aims
    # to reproduce it in these cases, and passes if it does so:
    recovery_expected = disposition in (
        WalFileDisposition.NODELETE,
        WalFileDisposition.PERSIST,
        WalFileDisposition.NO_CHECKPOINT,
    )

    sqlite3_errorlog_init()
    sqlite3_errorlog_read_logs()  # clear out any leftover error logs from other tests

    if disposition is WalFileDisposition.NODELETE:
        # Initialise nodeletefs_test, configured to prevent deletion of the
        # database WAL file:
        assert nodeletefs_init("nodeletefs_test", sqlite3_vfs_default(), wal_file) == SQLITE_OK
        underlying_vfs = "nodeletefs_test"
    else:
        underlying_vfs = sqlite3_vfs_default()

    # Stack vfstrace on top so that we can see what happens during the test,
    # especially detecting checkpoints:
    assert sqlite3_vfstrace_init("vfstrace_test", underlying_vfs) == 0

    def db_connection():
        with sqlite3.connect(f"file://{db_path}?vfs=vfstrace_test", uri=True) as db:
            # Enable SQLITE_FCNTL_PERSIST_WAL only if WalFileDisposition.PERSIST or
            # PERSIST_LIMIT:
            assert (
                sqlite3_set_persist_wal(
                    db,
                    "main",
                    (disposition in (WalFileDisposition.PERSIST, WalFileDisposition.PERSIST_LIMIT)),
                )
                == SQLITE_OK
            )

            if disposition is WalFileDisposition.PERSIST_LIMIT:
                db.execute("PRAGMA journal_size_limit=1000").close()

            # Enable SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE only if WalFileDisposition.NO_CHECKPOINT:
            sqlite3_db_config(
                db,
                SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                (1 if disposition is WalFileDisposition.NO_CHECKPOINT else 0),
            )
            return db

    db = db_connection()
    db.execute("PRAGMA journal_mode=WAL").close()
    db.execute("CREATE TABLE t (id TEXT)").close()
    db.execute("INSERT INTO t VALUES ('a')").close()

    def get_vfstrace_logs():
        return "".join(sqlite3_vfstrace_read_logs()).splitlines()

    # We search for this line in the vfstrace logs to find out whether a
    # checkpoint was done:
    db_filename = os.path.basename(db_path)
    checkpoint_indicator = f"vfstrace_test.xFileControl({db_filename},CKPT_DONE) -> SQLITE_NOTFOUND"

    # Close the database for the first time:
    db.close()

    # The first connection should not run recovery, so we should not see it in
    # the error_log:
    error_log = sqlite3_errorlog_read_logs()
    assert error_log == []

    vfstrace_logs = get_vfstrace_logs()
    # pprint(logs)
    # We always expect to see a checkpoint when the first connection is closed,
    # unless disposition was WalFileDisposition.NO_CHECKPOINT which disables it:
    if disposition is WalFileDisposition.NO_CHECKPOINT:
        assert checkpoint_indicator not in vfstrace_logs
    else:
        assert checkpoint_indicator in vfstrace_logs

    # Iff the WAL file exists after close, then we (unfortunately) expect
    # database recovery to happen on next open, so the reverse is also true.
    # Also, if disposition is PERSIST_LIMIT then, like PERSIST, the file will
    # still exist:
    assert os.path.exists(wal_file) == (recovery_expected or disposition is WalFileDisposition.PERSIST_LIMIT)

    # Reopen and close the database again:
    db = db_connection()
    db.execute("SELECT * FROM t").close()
    if disposition is WalFileDisposition.CHECKPOINT:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)").close()
    db.close()

    # What happened during the second connection? If recovery_expected then we
    # expect it to run recovery, which should log "recovered 2 frames from WAL file":
    error_log = sqlite3_errorlog_read_logs()
    if recovery_expected:
        assert error_log == [
            (283, f"recovered 2 frames from WAL file {db_path}-wal"),
        ]
    else:
        assert error_log == []

    vfstrace_logs = get_vfstrace_logs()
    # pprint(vfstrace_logs)
    # If recovery_expected and not NO_CHECKPOINT (which disables the checkpoint)
    # then we expect this recovery to cause another checkpoint on close, because
    # nBackfill was reset to 0:
    if recovery_expected and disposition is not WalFileDisposition.NO_CHECKPOINT:
        assert checkpoint_indicator in vfstrace_logs
    else:
        assert checkpoint_indicator not in vfstrace_logs
