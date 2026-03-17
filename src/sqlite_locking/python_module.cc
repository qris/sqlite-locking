#include <cstdint>
#include <list>
#include <sstream>
#include <string>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sqlite3.h>

extern "C" {
#include "connection.h"
}

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
bool sqlite3_db_config_wrapper(py::handle connection, int op, int enable)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());

    // Cannot check_thread or check_connection here, because they are private
    // (static) and rely on the internal structure of pysqlite_Connection,
    // which is also private and changes between Python versions! So just rely
    // on db being the first member of the structure, and hope that
    // sqlite3_db_config will handle it being NULL, and that the user is not
    // being naughty with threads!

    if (!is_int_config(op)) {
        throw std::invalid_argument("Unknown or unsupported config 'op'");
    }

    int actual;
    int result = sqlite3_db_config(self->db, op, enable, &actual);
    if (result != SQLITE_OK) {
        throw std::runtime_error("sqlite3_db_config failed");
    }
    return bool(actual);
}

/*
** An instance of this structure is attached to the each trace VFS to
** provide auxiliary information.
*/
typedef struct nodeletefs_info nodeletefs_info;
struct nodeletefs_info {
    sqlite3_vfs *pUnderlyingVfs;    /* The underlying real VFS */
    const char *vfs_name;           /* "VFS name" of this nodeletefs */
    const char *no_delete_filename; /* Name of the file to refuse to delete */
};

#define VFS_INFO(pVfs) ((nodeletefs_info *)((pVfs)->pAppData))
#define VFS_ORIG(pVfs) (VFS_INFO(pVfs)->pUnderlyingVfs)

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning. Overridden to prevent deletion in nodeletefs!
*/
static int nodeletefs_Delete(sqlite3_vfs *pVfs, const char *zPath, int dirSync)
{
    nodeletefs_info *pInfo = VFS_INFO(pVfs);
    if (strcmp(zPath, pInfo->no_delete_filename) == 0) {
        return SQLITE_IOERR_DELETE;
    }
    return VFS_ORIG(pVfs)->xDelete(VFS_ORIG(pVfs), zPath, dirSync);
}

/* We need to implement ALL VFS methods, even those that we don't intend to
 * use ourselves, because we need to pass the underlying VFS' structure to its
 * methods, so we need to dereference ours to get it.
 */

static int nodeletefs_Open(sqlite3_vfs *pVfs, /* VFS */
    const char *zName,   /* File to open, or 0 for a temp file */
    sqlite3_file *pFile, /* Pointer to DemoFile struct to populate */
    int flags,           /* Input SQLITE_OPEN_XXX flags */
    int *pOutFlags       /* Output SQLITE_OPEN_XXX flags (or NULL) */
)
{
    return VFS_ORIG(pVfs)->xOpen(
        VFS_ORIG(pVfs), zName, pFile, flags, pOutFlags);
}

static int nodeletefs_Access(
    sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut)
{
    return VFS_ORIG(pVfs)->xAccess(VFS_ORIG(pVfs), zPath, flags, pResOut);
}

static int nodeletefs_FullPathname(
    sqlite3_vfs *pVfs, const char *zPath, int nOut, char *zOut)
{
    return VFS_ORIG(pVfs)->xFullPathname(VFS_ORIG(pVfs), zPath, nOut, zOut);
}

static void *nodeletefs_DlOpen(sqlite3_vfs *pVfs, const char *zPath)
{
    return VFS_ORIG(pVfs)->xDlOpen(VFS_ORIG(pVfs), zPath);
}

static void nodeletefs_DlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
    VFS_ORIG(pVfs)->xDlError(VFS_ORIG(pVfs), nByte, zErrMsg);
}

static void (*nodeletefs_DlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(
    void)
{
    return VFS_ORIG(pVfs)->xDlSym(VFS_ORIG(pVfs), p, zSym);
}

static void nodeletefs_DlClose(sqlite3_vfs *pVfs, void *pHandle)
{
    VFS_ORIG(pVfs)->xDlClose(VFS_ORIG(pVfs), pHandle);
}

static int nodeletefs_Randomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut)
{
    return VFS_ORIG(pVfs)->xRandomness(VFS_ORIG(pVfs), nByte, zBufOut);
}

static int nodeletefs_Sleep(sqlite3_vfs *pVfs, int nMicro)
{
    return VFS_ORIG(pVfs)->xSleep(VFS_ORIG(pVfs), nMicro);
}

static int nodeletefs_CurrentTime(sqlite3_vfs *pVfs, double *pTimeOut)
{
    return VFS_ORIG(pVfs)->xCurrentTime(VFS_ORIG(pVfs), pTimeOut);
}

static int nodeletefs_GetLastError(sqlite3_vfs *pVfs, int a, char *b)
{
    return VFS_ORIG(pVfs)->xGetLastError(VFS_ORIG(pVfs), a, b);
}

static int nodeletefs_CurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *p)
{
    return VFS_ORIG(pVfs)->xCurrentTimeInt64(VFS_ORIG(pVfs), p);
}

/*
** Clients invoke this routine to construct a new nodeletefs shim.
**
** Return SQLITE_OK on success.
**
** SQLITE_NOMEM is returned in the case of a memory allocation error.
** SQLITE_NOTFOUND is returned if zOldVfsName does not exist.
*/
static int nodeletefs_register(
    const char *zNewVfsName,        /* Name of the newly constructed VFS */
    const char *zOldVfsName,        /* Name of the underlying VFS */
    const char *no_delete_filename, /* Name of file to refuse to delete */
    int makeDefault                 /* True to make the new VFS the default */
)
{
    sqlite3_vfs *pNew;
    sqlite3_vfs *pUnderlyingVfs;
    nodeletefs_info *pInfo;
    size_t nByte;

    pUnderlyingVfs = sqlite3_vfs_find(zOldVfsName);
    if (pUnderlyingVfs == 0)
        return SQLITE_NOTFOUND;
    nByte = sizeof(*pNew) + sizeof(*pInfo);
    pNew = (sqlite3_vfs *)sqlite3_malloc64(nByte);
    if (pNew == 0)
        return SQLITE_NOMEM;
    memset(pNew, 0, nByte);
    pInfo = (nodeletefs_info *)&pNew[1];
    pInfo->pUnderlyingVfs = pUnderlyingVfs;
    pInfo->vfs_name = strdup(zNewVfsName);
    pInfo->no_delete_filename = strdup(no_delete_filename);

    pNew->iVersion = pUnderlyingVfs->iVersion;
    pNew->szOsFile = pUnderlyingVfs->szOsFile;
    pNew->mxPathname = pUnderlyingVfs->mxPathname;
    pNew->zName = pInfo->vfs_name;
    pNew->pAppData = pInfo;

    pNew->xOpen = nodeletefs_Open;
    pNew->xDelete = nodeletefs_Delete;
    pNew->xAccess = nodeletefs_Access;
    pNew->xFullPathname = nodeletefs_FullPathname;
    pNew->xDlOpen = nodeletefs_DlOpen;
    pNew->xDlError = nodeletefs_DlError;
    pNew->xDlSym = nodeletefs_DlSym;
    pNew->xDlClose = nodeletefs_DlClose;
    pNew->xRandomness = nodeletefs_Randomness;
    pNew->xSleep = nodeletefs_Sleep;
    pNew->xCurrentTime = nodeletefs_CurrentTime;
    pNew->xGetLastError = nodeletefs_GetLastError;
    if (pNew->iVersion >= 2) {
        pNew->xCurrentTimeInt64 = nodeletefs_CurrentTimeInt64;
        /*
        if (pNew->iVersion >= 3) {
            pNew->xSetSystemCall = pRoot->xSetSystemCall;
            pNew->xGetSystemCall = pRoot->xGetSystemCall;
            pNew->xNextSystemCall = pRoot->xNextSystemCall;
        }
        */
    }

    return sqlite3_vfs_register(pNew, makeDefault);
}

/*
** Look for the named VFS.  If it is a TRACEVFS, then unregister it
** and delete it.
*/
static int nodeletefs_unregister(const char *vfs_name)
{
    sqlite3_vfs *pVfs = sqlite3_vfs_find(vfs_name);
    if (pVfs == 0)
        return SQLITE_NOTFOUND;

    if (pVfs->xDelete != nodeletefs_Delete)
        return SQLITE_MISMATCH;

    nodeletefs_info *pInfo = (nodeletefs_info *)pVfs->pAppData;
    free((void *)(pInfo->vfs_name));
    free((void *)(pInfo->no_delete_filename));
    sqlite3_vfs_unregister(pVfs);
    sqlite3_free(pVfs);
    return SQLITE_OK;
}

static int nodeletefs_init(const std::string &new_vfs_name,
    const std::string &old_vfs_name, const std::string &no_delete_filename)
{
    /**
     * Install the nodeletefs VFS, stacked on top of old_vfs_name (an existing
     * VFS).
     *
     * If old_vfs_name is empty, just unregister the nodeletefs named
     * new_vfs_name, if it exists.
     */
    int rc = nodeletefs_unregister(new_vfs_name.c_str());
    if (rc != SQLITE_OK && rc != SQLITE_NOTFOUND) {
        return rc;
    }
    if (!old_vfs_name.empty()) {
        return nodeletefs_register(new_vfs_name.c_str(), old_vfs_name.c_str(),
            no_delete_filename.c_str(), 0);
    }
    else {
        return rc; // might be SQLITE_OK or SQLITE_NOTFOUND
    }
}

/*
 * Delete a file (on disk) using the VFS method xDelete. Used to test that
 * nodeletefs is doing what it should.
 *
 * xDelete does not seem to be properly documented on this page:
 * <https://sqlite.org/c3ref/vfs.html>. But there is some test code here:
 * <https://sqlite.org/src/doc/trunk/src/test_vfs.c>, which says:
 * "Delete the file located at zPath. If the dirSync argument is true, ensure
 * the file-system modifications are synced to disk before returning."
 */
static int nodeletefs_delete_wrapper(py::handle connection,
    const std::string &schema_name, const std::string &filename)
{
    pysqlite_Connection *self = (pysqlite_Connection *)(connection.ptr());
    sqlite3 *db = self->db;
    sqlite3_vfs *pVfs = NULL;

    int rc = sqlite3_file_control(
        db, schema_name.c_str(), SQLITE_FCNTL_VFS_POINTER, &pVfs);
    if (rc != SQLITE_OK) {
        sqlite3_log(rc, "SQLITE_FCNTL_VFS_POINTER failed");
        return rc;
    }

    if (pVfs == NULL) {
        sqlite3_log(rc, "SQLITE_FCNTL_VFS_POINTER returned NULL");
        return SQLITE_NOMEM;
    }

    rc = pVfs->xDelete(pVfs, filename.c_str(), 0);
    if (rc != SQLITE_OK) {
        sqlite3_log(rc, "xDelete failed");
    }

    return rc;
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
    m.def("nodeletefs_init", &nodeletefs_init,
        "Create a new VFS using nodeletefs, with the specified name");
    m.def("nodeletefs_delete", &nodeletefs_delete_wrapper,
        "Delete a file (on disk) using the VFS method xDelete, for testing "
        "ONLY");

    // Based on
    // https://github.com/python/cpython/blob/0dfe649400a0b67318169ec813475f4949ad7b69/Modules/_sqlite/module.c#L444-L449
    // and https://github.com/pybind/pybind11/issues/92#issuecomment-178131592:
#define ADD_INT(ival)                                                          \
    do {                                                                       \
        m.attr(#ival) = py::int_(ival);                                        \
    } while (0);

    ADD_INT(SQLITE_OK);
    ADD_INT(SQLITE_MISMATCH);
    ADD_INT(SQLITE_MISUSE);
    ADD_INT(SQLITE_NOTFOUND);
    ADD_INT(SQLITE_IOERR);
    ADD_INT(SQLITE_IOERR_DELETE);
    ADD_INT(SQLITE_IOERR_DELETE_NOENT);

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
