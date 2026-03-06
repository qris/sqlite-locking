# SQLite Locking

[![Python package](https://github.com/qris/sqlite-locking/actions/workflows/python-package.yml/badge.svg)](https://github.com/qris/sqlite-locking/actions/workflows/python-package.yml)

Python native extensions used for low-level SQLite lock debugging.

## Installation

Add the [Git VCS spec](https://pip.pypa.io/en/stable/topics/vcs-support/) to
your project's dependencies, e.g. `pyproject.toml` or `requirements.txt`:

```
sqlite_locking @ git+https://github.com/qris/sqlite-locking.git
```

You can also reference a specific commit like this:

```
sqlite_locking @ git+https://github.com/qris/sqlite-locking.git@d1f77c60a703b2cce2ddbc5985afe1758325de4f
```

## Development

* Install [PDM](https://pdm-project.org/en/latest/).
* Run `pdm install -G dev` in the checked-out source to compile and install the module in the project venv.
* Run `pdm run pytest` to run the tests.
* Run `pdm run ruff check` to check for lint errors.
* Increase the version number in `pyproject.toml` before pushing back to the main repo.
