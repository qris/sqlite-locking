#include <cstdint>
#include <list>
#include <string>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sqlite3.h>

#include "connection.h"

namespace py = pybind11;

int sqlite3_txn_state_wrapper(py::handle connection, char* p_schema)
{
    pysqlite_Connection* self = (pysqlite_Connection *)(connection.ptr());
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
    pysqlite_Connection* self = (pysqlite_Connection *)(connection.ptr());
    return sqlite3_enable_load_extension(self->db, onoff);
}

py::tuple sqlite3_wal_checkpoint_v2_wrapper(
  py::handle connection,          /* Database handle */
  const char *zDb,                /* Name of attached database (or NULL) */
  int eMode                       /* SQLITE_CHECKPOINT_* value */
)
{
    pysqlite_Connection* self = (pysqlite_Connection *)(connection.ptr());
    int pnLog;                     /* OUT: Size of WAL log in frames */
    int pnCkpt;                    /* OUT: Total number of frames checkpointed */
    int result = sqlite3_wal_checkpoint_v2(self->db, zDb, eMode, &pnLog, &pnCkpt);
    return py::make_tuple(result, pnLog, pnCkpt);
}

py::tuple sqlite3_db_status_wrapper(py::handle connection, int op,
    bool reset_hwm)
{
    pysqlite_Connection* self = (pysqlite_Connection *)(connection.ptr());
    // sqlite3_db_status64 requires a newer version of sqlite than 3.45.1,
    // so use the old 32-bit version instead:
    int current_value;
    int high_water_value;
    int result = sqlite3_db_status(self->db, op, &current_value,
        &high_water_value, reset_hwm ? 1 : 0);
    return py::make_tuple(result, current_value, high_water_value);
}

// From https://sqlite.org/src/doc/trunk/ext/misc/vfstrace.c:
extern "C" int vfstrace_register(
    const char *zTraceName,         // Name of the newly constructed VFS
    const char *zOldVfsName,        // Name of the underlying VFS
    int (*xOut)(const char*,void*), // Output routine.  ex: fputs
    void *pOutArg,                  // 2nd argument to xOut.  ex: stderr
    int makeDefault                 // Make the new VFS the default
);

typedef std::list<std::string> log_t;
static log_t sqlite3_vfstrace_logs;

int sqlite3_vfstrace_append_log(const char *zMessage, void *pAppData)
{
    /**
     * Append one log message to the global log.
     */
    sqlite3_vfstrace_logs.push_back(std::string(zMessage));
    return 0; // not used
}

int sqlite3_vfstrace_init(const std::string& new_vfs_name,
                              const std::string& old_vfs_name)
{
    return vfstrace_register(new_vfs_name.c_str(), old_vfs_name.c_str(),
        sqlite3_vfstrace_append_log, NULL, 0);
}

log_t sqlite3_vfstrace_read_logs()
{
    /**
     * Return and clear the contents of the global log.
     */
    log_t tmp_copy = sqlite3_vfstrace_logs;
    sqlite3_vfstrace_logs.clear();
    return tmp_copy;
}

PYBIND11_MODULE(python_module, m) {
    m.doc() = "Python native extensions used for low-level SQLite lock debugging."; // optional module docstring
    m.def("sqlite3_txn_state", &sqlite3_txn_state_wrapper,
        "Gives direct access to the sqlite3_txn_state native function");
    m.def("sqlite3_enable_load_extension", &sqlite3_enable_load_extension_wrapper,
        "Gives direct access to the sqlite3_enable_load_extension native function");
    m.def("sqlite3_wal_checkpoint_v2", &sqlite3_wal_checkpoint_v2_wrapper,
        "Gives direct access to the sqlite3_wal_checkpoint_v2 native function");
    m.def("sqlite3_db_status", &sqlite3_db_status_wrapper,
        "Gives direct access to the sqlite3_db_status native function");
    m.def("sqlite3_errstr", &sqlite3_errstr,
        "Gives direct access to the sqlite3_errstr native function");
    m.def("sqlite3_vfstrace_init", &sqlite3_vfstrace_init,
        "Creates a new VFS using vfstrace, with the specified name");
    m.def("sqlite3_vfstrace_read_logs", &sqlite3_vfstrace_read_logs,
        "Returns logs accumulated by any of our vfstrace VFS instances");
}
