"""Utilities for working with SQLite Extensions in Python."""

import importlib
import os
import sqlite3

import sqlite_locking
from sqlite_locking.python_module import sqlite3_enable_load_extension


def load_extension(db: sqlite3.Connection, library_name: str):
    """
    Load an extension (by name) into a SQLite database.

    Args:
        db: the database to load the extension into.
        library_name: the base name of the extension shared library, which
            should be installed in the ram_native extension module.
    """
    library_file_path = os.path.join(os.path.dirname(sqlite_locking.__file__),
                                     library_name +
                                     importlib.machinery.EXTENSION_SUFFIXES[0])
    assert os.path.exists(library_file_path), (
        f'{library_file_path} not installed as expected with ram_native package'
    )

    # Normally you would use db.enable_load_extension() (or just
    # db.load_extension()), but Python 3.10.3 is too old and doesn't have either
    # of those functions exposed in sqlite3.Connection, so we use
    # python_module.sqlite3_enable_load_extension instead:
    # db.load_extension(extension_lib)
    sqlite3_enable_load_extension(db, 1)

    # load_extension raises a (SQL) exception if the extension fails to load or
    # initialize correctly, so we don't need to check its return value:
    db.execute('select load_extension(:lib_path)',
               {'lib_path': library_file_path.replace('.so', '')}).close()

    # Disable extension loading again, for security:
    sqlite3_enable_load_extension(db, 0)
