"""
Setuptools configuration for the sqlite_locking package.

https://setuptools.pypa.io/en/latest/userguide/ext_modules.html
"""

# setup.py (with automatic dependency tracking)

from pybind11.setup_helpers import Pybind11Extension
from setuptools import setup

# https://github.com/pybind/python_example/blob/master/setup.py

ext_modules = [
    Pybind11Extension(
        "sqlite_locking.python_module",
        ["src/sqlite_locking/vfsstat.c", "src/sqlite_locking/vfstrace.c", "src/sqlite_locking/python_module.cc"],
        extra_compile_args=["-std=c++11", "-Werror=all"],
        libraries=["sqlite3"],
        # Example: passing in the version to the compiled code
        # define_macros=[("VERSION_INFO", __version__)],
    ),
]

setup(
    ext_modules=ext_modules,
)
