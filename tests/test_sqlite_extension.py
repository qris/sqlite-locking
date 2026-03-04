"""Tests for sqlite_locking.sqlite_extension."""

from contextlib import closing
import logging
import os
import sqlite3

from multiprocessing import Event, Process
import time

from more_itertools import one
import pytest
from sqlite_locking.enums import SqliteLockState, TransactionState
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

def test_sqlite3_txn_and_lock_state(db_extension, tmp_path):
    """
    Tests for sqlite3_txn_state, sqlite3_lock_state and sqlite3_check_reserved_lock.

    These are custom SQL functions defined in the sqlite3_ram_ext extension.
    """
    db = db_extension
    db.execute('PRAGMA journal_mode=WAL')

    def get_txn_state():
        return one(one(db.execute('select sqlite3_txn_state(NULL);').fetchall()))

    def get_lock_state(schema=None):
        return one(one(db.execute('select sqlite3_lock_state(:schema)',
                                    {'schema': schema}).fetchall()))

    def get_reserved_lock(schema=None):
        return one(one(db.execute('select sqlite3_check_reserved_lock(:schema)',
                                    {'schema': schema}).fetchall()))

    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert TransactionState.current(db) is TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.NONE
    assert not get_reserved_lock()
    assert not db.in_transaction

    # Attach another database so we can pass its schema to get_lock_state below:
    another_db_path = os.path.join(tmp_path, 'database_2.tmp')
    db.execute('ATTACH :another_db_path AS another_db',
               {'another_db_path': another_db_path})

    # This should not have changed any state in the main connection:
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.NONE
    assert not get_reserved_lock()

    # In autocommit mode, every statement commits immediately, so we are not
    # in a transaction afterwards:
    db.execute('CREATE TABLE test (id text)')
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # Explicit BEGIN does not actually start a transaction (but it does make
    # in_transaction return True):
    db.execute('BEGIN')
    assert db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # But touching the database starts one:
    db.execute('SELECT * FROM test')
    assert db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_READ
    assert TransactionState.current(db) is TransactionState.SQLITE_TXN_READ
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # Upgrade to WRITE transaction:
    db.execute("INSERT INTO test VALUES ('test_1')")
    assert db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_WRITE
    assert TransactionState.current(db) is TransactionState.SQLITE_TXN_WRITE
    assert get_lock_state() == SqliteLockState.SHARED  # not promoted yet
    assert get_lock_state(schema='another_db') == SqliteLockState.NONE
    assert not get_reserved_lock()

    # And COMMIT ends it:
    db.execute('COMMIT')
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert TransactionState.current(db) is TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # Explicit BEGIN EXCLUSIVE does immediately start a transaction with an
    # exclusive lock
    db.execute('BEGIN EXCLUSIVE')
    assert db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_WRITE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()  # still not, because lock not promoted yet!

    # Because it's hard to get the database to call us back while in the middle
    # of a COMMIT with a real write lock held, we use sqlite3_xlock to force it!
    # This would be a bad idea on a real database:
    db.execute('select sqlite3_xlock(NULL, :new_level)',
               {'new_level': SqliteLockState.RESERVED})
    assert get_lock_state() == SqliteLockState.RESERVED
    assert get_reserved_lock()

    db.execute('select sqlite3_xunlock(NULL, :new_level)',
               {'new_level': SqliteLockState.SHARED})
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # And COMMIT ends it:
    db.execute('COMMIT')
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    # In locking_mode EXCLUSIVE, the lock is not given up after COMMIT:
    db.execute('PRAGMA locking_mode = EXCLUSIVE')
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.SHARED
    assert not get_reserved_lock()

    db.execute('BEGIN EXCLUSIVE')
    assert db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_WRITE
    assert get_lock_state() == SqliteLockState.EXCLUSIVE
    assert get_reserved_lock()

    db.execute('COMMIT')
    assert not db.in_transaction
    assert get_txn_state() == TransactionState.SQLITE_TXN_NONE
    assert get_lock_state() == SqliteLockState.EXCLUSIVE
    assert get_reserved_lock()


def test_sqlite3_stmt_readonly(db_extension):
    """Tests for the sqlite3_stmt_readonly custom function in sqlite3_ram_ext."""
    db = db_extension

    def get_sqlite3_stmt_readonly(sql):
        return one(one(db.execute('select sqlite3_stmt_readonly(:sql)',
                                    {'sql': sql}).fetchall()))

    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_stmt_readonly(None)
    assert str(exc_info.value) == 'SQL statement text is NULL'

    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_stmt_readonly("FOO")
    assert str(exc_info.value) == 'SQL statement text is not valid SQL (1)'

    with pytest.raises(sqlite3.IntegrityError) as exc_info:
        get_sqlite3_stmt_readonly("SELECT 1'\0'")
    assert str(exc_info.value) == 'SQL statement text contains a NUL byte'

    # Read-only statements:
    assert get_sqlite3_stmt_readonly("SELECT 1") != 0
    # Containing bound parameters should not make it invalid SQL:
    assert get_sqlite3_stmt_readonly("SELECT :foo") != 0

    # Read-write statements:
    assert get_sqlite3_stmt_readonly("CREATE TABLE foo (id int4)") == 0
    # A valid statement referencing a nonexistent object will fail:
    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_stmt_readonly("DELETE FROM foo WHERE id = 0")
    assert str(exc_info.value) == 'SQL statement text is not valid SQL (1)'

    # So create it:
    db.execute("CREATE TABLE foo (id int4)")
    assert get_sqlite3_stmt_readonly("DELETE FROM foo WHERE id = 0") == 0
    # Containing bound parameters should not make it invalid SQL:
    assert get_sqlite3_stmt_readonly("DELETE FROM foo WHERE id = :bar") == 0


def test_sqlite3_normalized_sql(db_extension):
    """Tests for the sqlite3_normalized_sql custom function in sqlite3_ram_ext."""
    db = db_extension

    def get_sqlite3_normalized_sql(sql):
        return one(one(db.execute('select sqlite3_normalized_sql(:sql)',
                                    {'sql': sql}).fetchall()))

    with pytest.raises(sqlite3.InternalError) as exc_info:
        get_sqlite3_normalized_sql(None)
    assert str(exc_info.value) == 'sqlite3_normalized_sql not compiled in'
    return  # cannot test anything else

    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_normalized_sql(None)
    assert str(exc_info.value) == 'SQL statement text is NULL'

    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_normalized_sql("FOO")
    assert str(exc_info.value) == 'SQL statement text is not valid SQL (1)'

    with pytest.raises(sqlite3.IntegrityError) as exc_info:
        get_sqlite3_normalized_sql("SELECT 1'\0'")
    assert str(exc_info.value) == 'SQL statement text contains a NUL byte'

    # Read-only statements:
    assert get_sqlite3_normalized_sql("SELECT 1") == 'SELECT 1'
    # Containing bound parameters should not make it invalid SQL:
    assert get_sqlite3_normalized_sql("SELECT :foo") == 'SELECT :foo'

    # Read-write statements:
    assert get_sqlite3_normalized_sql("CREATE TABLE foo (id int4)") == \
        'CREATE TABLE foo (id int4)'
    # A valid statement referencing a nonexistent object will fail:
    with pytest.raises(sqlite3.OperationalError) as exc_info:
        get_sqlite3_normalized_sql("DELETE FROM foo WHERE id = 0")
    assert str(exc_info.value) == 'SQL statement text is not valid SQL (1)'

    # So create it:
    db.execute("CREATE TABLE foo (id int4)")
    assert get_sqlite3_normalized_sql("DELETE FROM foo WHERE id = 0") == \
        "DELETE FROM foo WHERE id = 0"
    # Containing bound parameters should not make it invalid SQL:
    assert get_sqlite3_normalized_sql("DELETE FROM foo WHERE id = :bar") == \
        "DELETE FROM foo WHERE id = :bar"

    # Now check what we really care about: that COMMIT in various forms is
    # normalized
    assert get_sqlite3_normalized_sql("COMMIT") == 'COMMIT'
    assert get_sqlite3_normalized_sql(" COMMIT") == 'COMMIT'
    assert get_sqlite3_normalized_sql(" COMMIT;") == 'COMMIT'
    assert get_sqlite3_normalized_sql("SELECT 'COMMIT';") == "SELECT 'COMMIT'"


@pytest.mark.parametrize('wal_mode', [False, True])
def test_sqlite3_check_reserved_lock_multiprocess(wal_mode, db_extension, db_path):
    """
    Tests for the sqlite3_check_reserved_lock custom function in sqlite3_ram_ext.

    We tested above how it behaves with a lock in the current process (basically
    it's never reserved because that only happens during COMMIT), but here we
    check what happens when another process (db_opener_worker) has locked the
    database.
    """
    db = db_extension

    if wal_mode:
        db.execute('PRAGMA journal_mode=WAL')

    def get_external_reader(schema=None):
        with closing(db.execute('select sqlite3_external_reader(:schema)',
                     {'schema': schema})) as cursor:
            return one(one(cursor.fetchall()))

    def get_reserved_lock(schema=None):
        with closing(db.execute('select sqlite3_check_reserved_lock(:schema)',
                     {'schema': schema})) as cursor:
            return one(one(cursor.fetchall()))

    assert not get_external_reader(), "should be no external reader yet"
    assert not get_reserved_lock(), "should be no reserved lock yet"

    # To synchronize with our worker process:
    running_event = Event()

    # Start another process to open the database exclusively for a while:
    def db_opener_fn():
        """Opens the database exclusively and keeps it open for a while."""
        with sqlite3.connect(db_path) as db:
            logger.debug("Locking database file")
            db.execute('BEGIN EXCLUSIVE')
            running_event.set()
            time.sleep(120)
            logger.debug("Unlocking database file")

    db_opener_worker = Process(target=db_opener_fn)
    db_opener_worker.start()
    try:
        assert running_event.wait(5), "db_opener_fn did not open the database in time"

        # Check that it is actually locked:
        db.execute('PRAGMA busy_timeout = 0')
        with pytest.raises(sqlite3.OperationalError):
            db.execute('BEGIN EXCLUSIVE')

        assert not get_external_reader() == (not wal_mode), "should be no external reader in WAL mode"
        assert get_reserved_lock() == (not wal_mode), "should not be RESERVED in WAL mode"
        db_opener_worker.terminate()
        db_opener_worker.join()
        assert not get_external_reader(), "should be no external reader any more"
        assert not get_reserved_lock(), "should still not be RESERVED"
    finally:
        db_opener_worker.terminate()
        db_opener_worker.join()

