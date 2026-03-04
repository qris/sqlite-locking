"""Enumerations to make Python code more readable."""

from enum import Enum, IntEnum
import sqlite3
from typing import Optional

from sqlite_locking.python_module import sqlite3_txn_state, sqlite3_db_status


class TransactionState(IntEnum):
    """Transaction states that can be returned by sqlite3_txn_state."""

    SQLITE_TXN_NONE = 0
    SQLITE_TXN_READ = 1
    SQLITE_TXN_WRITE = 2

    @classmethod
    def current(cls, db: sqlite3.Connection, schema_name: Optional[str] = None):
        """Return the current TransactionState for a given SQLite database."""
        return cls(sqlite3_txn_state(db, schema_name))


class SqliteLockState(IntEnum):
    """
    SQLite C Interface: File Locking Levels.

    These are the conceptual lock levels that a process can hold on an open
    SQLite database at one time. See <https://sqlite.org/lockingv3.html> and
    <https://sqlite.org/atomiccommit.html> for when and how these lock levels
    are used.

    A connection can only be in one of these at a time, although different
    processes or connections to the database could be in the same or different
    states at one time.

    These (numeric) values are returned by the xCheckReservedLock function in
    the SQLite unix vfs. It's possible to access these by calling the C function
    directly using ctypes, or loading a custom extension into SQLite which
    defines new SQL functions, such as "sqlite3_lock_state()" defined by
    "sqlite_extension.c". See test_sqlite3_txn_and_lock_state for details.

    These constant names and values are copied from
    <https://sqlite.org/c3ref/c_lock_exclusive.html>.
    """

    NONE      = 0
    SHARED    = 1
    RESERVED  = 2
    PENDING   = 3
    EXCLUSIVE = 4

    def __str__(self):
        """Return short name without quotes."""
        return self.name

    def __repr__(self):
        """
        Return qualified name.

        We don't really care to see the numeric value every time, this isn't C.
        """
        return f"{self.__class__.__name__}.{self.name}"


class SqliteDatabaseStatus(Enum):
    """
    Possible values of the op argument to sqlite3_db_status.

    https://sqlite.org/c3ref/c_dbstatus_options.html
    """
    LOOKASIDE_USED      = 0
    CACHE_USED          = 1
    SCHEMA_USED         = 2
    STMT_USED           = 3
    LOOKASIDE_HIT       = 4
    LOOKASIDE_MISS_SIZE = 5
    LOOKASIDE_MISS_FULL = 6
    CACHE_HIT           = 7
    CACHE_MISS          = 8
    CACHE_WRITE         = 9
    DEFERRED_FKS        = 10
    CACHE_USED_SHARED   = 11
    CACHE_SPILL         = 12
    TEMPBUF_SPILL       = 13

    def get(self, db: sqlite3.Connection) -> int:
        """Return the specified value from the supplied database Connection."""
        result = sqlite3_db_status(db, self.value, False)
        if result[0] != 0:
            raise ValueError(f"sqlite3_db_status({self.name}) failed with "
                f"rc={result[0]}")
        return result[1]
