# SQLite Locking

[![Python package](https://github.com/qris/sqlite-locking/actions/workflows/python-package.yml/badge.svg)](https://github.com/qris/sqlite-locking/actions/workflows/python-package.yml)

This is a Python native extension containing low-level SQLite tools.

This provides access to SQLite internals that are not exposed by the `sqlite3`
module and not accessible in Python.

It contains a SQLite loadable extension which exposes some SQLite API functions
as SQL functions, so you can call them with SELECT queries:

* `sqlite3_txn_state`
* `sqlite3_lock_state` (SQLITE_FCNTL_LOCKSTATE)
* `sqlite3_check_reserved_lock`
* `sqlite3_stmt_readonly`
* `sqlite3_external_reader` (SQLITE_FCNTL_EXTERNAL_READER)
* `sqlite3_xlock` and `sqlite3_xunlock` (VFS xLock and xUnlock)

And a Python module containing functions that you can call directly from Python,
and some SQLite constants:

* sqlite3_txn_state: as above.
* sqlite3_enable_load_extension: replacement for
  [`sqlite3.Connection.enable_load_extension`](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.enable_load_extension),
  which is not available in older Python versions.
* sqlite3_wal_checkpoint_v2: calls the native function.
* sqlite3_db_status: calls the native function.
* sqlite3_errstr: calls the native function.
* sqlite3_vfstrace_init: initialises the bundled `vfstrace` VFS extension.
* sqlite3_vfstrace_read_logs: returns the vfstrace logs collected by that extension.
* sqlite3_errorlog_init: installs our own sqlite3 errorlog callback. In older
  versions of Python (sqlite3 < 3.42.0) this must be called before importing
  `sqlite3`, or it will fail and return SQLITE_MISUSE (21).
* sqlite3_errorlog_read_logs: returns the logs collected by that callback.
* sqlite3_log: calls the native function.
* sqlite3_vfs_default: returns the name of the current default VFS (e.g. `unix`).
* sqlite3_db_config: calls the native function.
* sqlite3_set_persist_wal: sets or clears `SQLITE_FCNTL_PERSIST_WAL`.
* nodeletefs_init: initialises the `nodeletefs` VFS.
* nodeletefs_delete: calls the `xDelete` VFS method.

These are power tools and they are dangerous to call. You should know exactly
what you're doing before calling them, or risk crashing your application or
corrupting your database. The best way to see how to use them is to look at the
[tests](https://github.com/qris/sqlite-locking/tree/main/tests).

In most cases, they are functions that you could call directly if you were using
the C API to SQLite, but you can't because you're using the Python `sqlite3`
module which exposes a much more restricted interface.

This code was developed to help investigate and solve a
SQLite locking issue, which turned out to be caused by excessive WAL backfilling
(on checkpoint at disconnect, while holding an EXCLUSIVE lock)
caused by [SQLite running recovery too often](https://sqlite.org/forum/forumpost/d8b23175b9).
I'm releasing it as open source because it is not part of our core business, and
it may be useful to others (save time debugging). However I do so with
**absolutely no support**, not even any intention of reading or merging pull
requests. You should not use it unless you understand the code and are willing
to support it yourself.

## Technology choices

I tried [several approaches](https://realpython.com/python-bindings-overview/)
before settling on `pybind11`:

* ctypes won't let us access the C pointer to `struct sqlite3` which is embedded
  inside the `Connection` `PyObject` created by `sqlite3`, which is required by
  almost all SQLite APIs.
* Cython.
* Numba doesn't give us access to the compiled object file, so can't create a
  SQLite Extension that way.
* CFFI: can't pass Python objects (e.g. `sqlite3.Connection`).

`pybind11` automatically checks and converts Python types to C++ types in
function arguments and return values, which is really nice. It allowed me to
create this module using an absolutely minimal amount of very standard C++ code.

It doesn't help that the CPython doesn't export the `sqlite3` headers, so we
have to rely on embedded copies of them, and the structures change significantly
between Python versions.

I tried to build the package purely with `pyproject.toml` but that seems only to
allow declaring `ext-modules` which are `setuptools.Extension`s, not
`Pybind11Extension`s. So I had to include a `setup.py` as well for that.

## Installation

I tried to make a GitHub Action to publish releases automatically to PyPI, but
the [Upload Python Package workflow](https://github.com/qris/sqlite-locking/actions/workflows/python-publish.yml)
always seems to fail with this error, which I don't have time to debug:

```
Getting PyPI token via OIDC...
Failed to get PyPI token via OIDC
[PdmUsageError]: Username and password are required
```

So for now, you'll have to install it by VCS URL in Pip.

Add the [Git VCS spec](https://pip.pypa.io/en/stable/topics/vcs-support/) to
your project's dependencies, e.g. `pyproject.toml` or `requirements.txt`:

```
sqlite_locking @ git+https://github.com/qris/sqlite-locking.git
```

You can also reference a specific tagged release like this:

```
sqlite_locking @ https://github.com/qris/sqlite-locking/archive/refs/tags/20260319-1.tar.gz
```

Or a specific commit:

```
sqlite_locking @ git+https://github.com/qris/sqlite-locking.git@d1f77c60a703b2cce2ddbc5985afe1758325de4f
```

## Development

* Install [PDM](https://pdm-project.org/en/latest/).
* Run `pdm install -G dev` in the checked-out source to compile and install the module in the project venv.
* Run `pdm run pytest` to run the tests.
* Run `pdm run ruff check` to check for lint errors.
* Increase the version number in `pyproject.toml` before pushing back to the main repo.
