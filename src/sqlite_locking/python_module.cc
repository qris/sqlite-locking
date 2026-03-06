#include <cstdint>
#include <list>
#include <string>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sqlite3.h>

#include "connection.h"

namespace py = pybind11;

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
}
