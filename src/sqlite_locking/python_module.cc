#include <cstdint>
#include <list>
#include <string>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sqlite3.h>

extern "C" {
#include "connection.h"
}

namespace py = pybind11;

/*
 * Checks if a connection object belongs to a different thread.
 *
 * false => error; true => ok
 *
 * Directly copied from CPython 3.12 cpython/Modules/_sqlite/connection.c.
 */

static bool _check_thread(pysqlite_Connection *self)
{
    if (self->check_same_thread) {
        if (PyThread_get_thread_ident() != self->thread_ident) {
            return false;
        }
    }
    return true;
}

/*
 * Checks if a connection object is usable (i. e. not closed).
 *
 * false => error; true => ok
 *
 * Directly copied from CPython 3.12 cpython/Modules/_sqlite/connection.c.
 */
static bool _check_connection(pysqlite_Connection *con)
{
    if (!con->initialized) {
        return false;
    }

    if (!con->db) {
        return false;
    }
    else {
        return true;
    }
}

int sqlite3_txn_state_wrapper(py::handle connection, char *p_schema)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    return sqlite3_txn_state(self->db, p_schema);
}

int sqlite3_enable_load_extension_wrapper(py::handle connection, int onoff)
{
    /*
    Gives direct access to the sqlite3_enable_load_extension native function.

    Normally you would use db.enable_load_extension() (or just
    db.load_extension()), but Python 3.10.3 on our server is too old and doesn't
    have either of those functions exposed in sqlite3.Connection, so we access
    them this way instead.
    */
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    return sqlite3_enable_load_extension(self->db, onoff);
}

py::tuple sqlite3_wal_checkpoint_v2_wrapper(
    py::handle connection, /* Database handle */
    const char *zDb,       /* Name of attached database (or NULL) */
    int eMode              /* SQLITE_CHECKPOINT_* value */
)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    int pnLog;  /* OUT: Size of WAL log in frames */
    int pnCkpt; /* OUT: Total number of frames checkpointed */
    int result =
        sqlite3_wal_checkpoint_v2(self->db, zDb, eMode, &pnLog, &pnCkpt);
    return py::make_tuple(result, pnLog, pnCkpt);
}

py::tuple sqlite3_db_status_wrapper(
    py::handle connection, int op, bool reset_hwm)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    // sqlite3_db_status64 requires a newer version of sqlite than 3.45.1,
    // so use the old 32-bit version instead:
    int current_value;
    int high_water_value;
    int result = sqlite3_db_status(
        self->db, op, &current_value, &high_water_value, reset_hwm ? 1 : 0);
    return py::make_tuple(result, current_value, high_water_value);
}

// From https://sqlite.org/src/doc/trunk/ext/misc/vfstrace.c:
extern "C" int vfstrace_register(
    const char *zTraceName,            // Name of the newly constructed VFS
    const char *zOldVfsName,           // Name of the underlying VFS
    int (*xOut)(const char *, void *), // Output routine.  ex: fputs
    void *pOutArg,                     // 2nd argument to xOut.  ex: stderr
    int makeDefault                    // Make the new VFS the default
);

typedef std::list<std::string> vfstrace_log_t;
static vfstrace_log_t sqlite3_vfstrace_logs;

int sqlite3_vfstrace_append_log(const char *zMessage, void *pAppData)
{
    /**
     * Append one log message to the global vfstrace log.
     */
    sqlite3_vfstrace_logs.push_back(std::string(zMessage));
    return 0; // not used
}

int sqlite3_vfstrace_init(
    const std::string &new_vfs_name, const std::string &old_vfs_name)
{
    /**
     * Install the vfstrace VFS, stacked on top of old_vfs_name (an existing
     * VFS).
     */
    return vfstrace_register(new_vfs_name.c_str(), old_vfs_name.c_str(),
        sqlite3_vfstrace_append_log, NULL, 0);
}

vfstrace_log_t sqlite3_vfstrace_read_logs()
{
    /**
     * Return and clear the contents of the global vfstrace log.
     */
    auto tmp_copy = sqlite3_vfstrace_logs;
    sqlite3_vfstrace_logs.clear();
    return tmp_copy;
}

typedef std::pair<int, std::string> errorlog_item;
typedef std::list<errorlog_item> errorlog_t;
static errorlog_t sqlite3_errorlog_logs;

int sqlite3_errorlog_append_log(void *pArg, int iErrCode, const char *zMsg)
{
    /**
     * Append one log message to the global errorlog.
     */
    assert(zMsg != NULL);
    errorlog_item new_item(iErrCode, std::string(zMsg));
    sqlite3_errorlog_logs.push_back(new_item);
    return 0; // not used
}

int sqlite3_errorlog_init()
{
    return sqlite3_config(SQLITE_CONFIG_LOG, sqlite3_errorlog_append_log, NULL);
}

errorlog_t sqlite3_errorlog_read_logs()
{
    /**
     * Return and clear the contents of the global errorlog.
     */
    auto tmp_copy = sqlite3_errorlog_logs;
    sqlite3_errorlog_logs.clear();
    return tmp_copy;
}

void sqlite3_log_wrapper(int iErrCode, const char *zMsg)
{
    /**
     * Write a message into the SQLite error log, for testing ONLY.
     *
     * Calls sqlite3_log, see <https://sqlite.org/c3ref/log.html> for details.
     * This logged message should be visible in the output of
     * sqlite3_errorlog_read_logs. This wrapper is needed because pybind11 can't
     * call varargs functions like sqlite3_log directly.
     *
     * "The sqlite3_log() interface is intended for use by extensions such as
     * virtual tables, collating functions, and SQL functions. While there is
     * nothing to prevent an application from calling sqlite3_log(), doing so is
     * considered bad form." So don't use this! (Using it in tests is OK.)
     */
    sqlite3_log(iErrCode, zMsg); // no varargs!
}

std::string sqlite3_vfs_default()
{
    /**
     * Return the name of the default VFS.
     */
    sqlite3_vfs *default_vfs = sqlite3_vfs_find(NULL);
    assert(default_vfs != NULL);
    return default_vfs->zName;
}

// Directly copied from CPython 3.12 cpython/Modules/_sqlite/connection.c:
static inline bool is_int_config(const int op)
{
    switch (op) {
    case SQLITE_DBCONFIG_ENABLE_FKEY:
    case SQLITE_DBCONFIG_ENABLE_TRIGGER:
    case SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER:
    case SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION:
#if SQLITE_VERSION_NUMBER >= 3016000
    case SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE:
#endif
#if SQLITE_VERSION_NUMBER >= 3020000
    case SQLITE_DBCONFIG_ENABLE_QPSG:
#endif
#if SQLITE_VERSION_NUMBER >= 3022000
    case SQLITE_DBCONFIG_TRIGGER_EQP:
#endif
#if SQLITE_VERSION_NUMBER >= 3024000
    case SQLITE_DBCONFIG_RESET_DATABASE:
#endif
#if SQLITE_VERSION_NUMBER >= 3026000
    case SQLITE_DBCONFIG_DEFENSIVE:
#endif
#if SQLITE_VERSION_NUMBER >= 3028000
    case SQLITE_DBCONFIG_WRITABLE_SCHEMA:
#endif
#if SQLITE_VERSION_NUMBER >= 3029000
    case SQLITE_DBCONFIG_DQS_DDL:
    case SQLITE_DBCONFIG_DQS_DML:
    case SQLITE_DBCONFIG_LEGACY_ALTER_TABLE:
#endif
#if SQLITE_VERSION_NUMBER >= 3030000
    case SQLITE_DBCONFIG_ENABLE_VIEW:
#endif
#if SQLITE_VERSION_NUMBER >= 3031000
    case SQLITE_DBCONFIG_LEGACY_FILE_FORMAT:
    case SQLITE_DBCONFIG_TRUSTED_SCHEMA:
#endif
        return true;
    default:
        return false;
    }
}

// Directly copied from CPython 3.12 cpython/Modules/_sqlite/connection.c:
py::tuple sqlite3_db_config_wrapper(py::handle connection, int op, int enable)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    if (!_check_thread(self) || !_check_connection(self)) {
        return py::make_tuple(SQLITE_MISUSE, "Invalid connection");
        ;
    }
    if (!is_int_config(op)) {
        return py::make_tuple(
            SQLITE_MISUSE, "Unknown or unsupported config 'op'");
    }

    int actual;
    int result = sqlite3_db_config(self->db, op, enable, &actual);
    if (result != SQLITE_OK) {
        return py::make_tuple(result, "sqlite3_db_config failed");
    }
    return py::make_tuple(result, actual);
}

PYBIND11_MODULE(python_module, m)
{
    m.doc() = "Python native extensions used for low-level SQLite lock "
              "debugging."; // optional module docstring
    m.def("sqlite3_txn_state", &sqlite3_txn_state_wrapper,
        "Call the sqlite3_txn_state native function");
    m.def("sqlite3_enable_load_extension",
        &sqlite3_enable_load_extension_wrapper,
        "Call the sqlite3_enable_load_extension native function");
    m.def("sqlite3_wal_checkpoint_v2", &sqlite3_wal_checkpoint_v2_wrapper,
        "Call the sqlite3_wal_checkpoint_v2 native function");
    m.def("sqlite3_db_status", &sqlite3_db_status_wrapper,
        "Call the sqlite3_db_status native function");
    m.def("sqlite3_errstr", &sqlite3_errstr,
        "Call the sqlite3_errstr native function");
    m.def("sqlite3_vfstrace_init", &sqlite3_vfstrace_init,
        "Create a new VFS using vfstrace, with the specified name");
    m.def("sqlite3_vfstrace_read_logs", &sqlite3_vfstrace_read_logs,
        "Return and clear the contents of the global vfstrace log");
    m.def("sqlite3_errorlog_init", &sqlite3_errorlog_init,
        "Install our sqlite3 errorlog callback");
    m.def("sqlite3_errorlog_read_logs", &sqlite3_errorlog_read_logs,
        "Return and clear the contents of the global errorlog");
    m.def("sqlite3_log", &sqlite3_log_wrapper,
        "Write a message into the SQLite error log, for testing ONLY");
    m.def("sqlite3_vfs_default", &sqlite3_vfs_default,
        "Return the name of the default VFS");
    m.def("sqlite3_db_config", &sqlite3_db_config_wrapper,
        "Call the sqlite3_db_config native function");

    // Based on
    // https://github.com/python/cpython/blob/0dfe649400a0b67318169ec813475f4949ad7b69/Modules/_sqlite/module.c#L444-L449
    // and https://github.com/pybind/pybind11/issues/92#issuecomment-178131592:
#define ADD_INT(ival)                                                          \
    do {                                                                       \
        m.attr(#ival) = py::int_(ival);                                        \
    } while (0);

    ADD_INT(SQLITE_DBCONFIG_ENABLE_FKEY);
    ADD_INT(SQLITE_DBCONFIG_ENABLE_TRIGGER);
    ADD_INT(SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER);
    ADD_INT(SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION);
#if SQLITE_VERSION_NUMBER >= 3016000
    ADD_INT(SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE);
#endif
#if SQLITE_VERSION_NUMBER >= 3020000
    ADD_INT(SQLITE_DBCONFIG_ENABLE_QPSG);
#endif
#if SQLITE_VERSION_NUMBER >= 3022000
    ADD_INT(SQLITE_DBCONFIG_TRIGGER_EQP);
#endif
#if SQLITE_VERSION_NUMBER >= 3024000
    ADD_INT(SQLITE_DBCONFIG_RESET_DATABASE);
#endif
#if SQLITE_VERSION_NUMBER >= 3026000
    ADD_INT(SQLITE_DBCONFIG_DEFENSIVE);
#endif
#if SQLITE_VERSION_NUMBER >= 3028000
    ADD_INT(SQLITE_DBCONFIG_WRITABLE_SCHEMA);
#endif
#if SQLITE_VERSION_NUMBER >= 3029000
    ADD_INT(SQLITE_DBCONFIG_DQS_DDL);
    ADD_INT(SQLITE_DBCONFIG_DQS_DML);
    ADD_INT(SQLITE_DBCONFIG_LEGACY_ALTER_TABLE);
#endif
#if SQLITE_VERSION_NUMBER >= 3030000
    ADD_INT(SQLITE_DBCONFIG_ENABLE_VIEW);
#endif
#if SQLITE_VERSION_NUMBER >= 3031000
    ADD_INT(SQLITE_DBCONFIG_LEGACY_FILE_FORMAT);
    ADD_INT(SQLITE_DBCONFIG_TRUSTED_SCHEMA);
#endif
#undef ADD_INT
}
