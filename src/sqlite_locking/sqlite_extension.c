/*
 * A loadable extension module for SQLite.
 *
 * When loaded, this adds SQL functions such as:
 *
 * sqlite3_txn_state()
 * sqlite3_stmt_readonly()
 * 
 * These functions are useful for debugging locking issues. Once the extension
 * is loaded, you can call them with e.g. `SELECT sqlite3_txn_state`. See 
 * test_database.py for usage and examples.
 *
 * Based on the sample extension at <https://sqlite.org/loadext.html>. Compiling
 * this native C module (with gcc) requires libsqlite3-ext (Debian/Ubuntu) to be
 * installed to provide sqlite3ext.h.
 *
 * Portions based on pysqlite_statement_create in statement.c of Python 3.15:
 * Copyright (C) 2005-2010 Gerhard Häring <gh@ghaering.de>
 *
 * This file is part of pysqlite.
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 *
 * Chris Wilson, 2025-11-11.
 */

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

/*
** Implementation of the "sqlite3_txn_state(schema)" SQL function. This returns
** the current transaction type (NONE, READ or WRITE) for the given schema
** (attached database), which is not exposed by the sqlite3 Python module.
** See <https://sqlite.org/c3ref/txn_state.html>.
*/
static void sqlite3_txn_state_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 1);
  sqlite3 * db = sqlite3_context_db_handle(context);
  int result;

  if (argc == 1)
  {
    const unsigned char * schema_name = sqlite3_value_text(argv[0]);
    result = sqlite3_txn_state(db, (const char *)schema_name);
  }
  else
  {
    result = sqlite3_txn_state(db, NULL);
  }

  sqlite3_result_int(context, result);
}

static sqlite3_stmt *get_sql_statement(sqlite3_context *context,
  sqlite3_value *arg)
{
  sqlite3 * db = sqlite3_context_db_handle(context);

  int sql_length = sqlite3_value_bytes(arg);
  int max_length = sqlite3_limit(db, SQLITE_LIMIT_SQL_LENGTH, -1);
  if (sql_length > max_length)
  {
    sqlite3_result_error(context, "SQL statement text is too long", -1);
    sqlite3_result_error_toobig(context);
    return NULL;
  }

  const char * sql_stmt_text = (const char *)sqlite3_value_text(arg); 
  if (sql_stmt_text == NULL)
  {
    sqlite3_result_error(context, "SQL statement text is NULL", -1);
    sqlite3_result_error_code(context, SQLITE_EMPTY);
    return NULL;
  }

  if (strlen(sql_stmt_text) != (size_t)sql_length)
  {
    sqlite3_result_error(context, "SQL statement text contains a NUL byte", -1);
    sqlite3_result_error_code(context, SQLITE_MISMATCH);
    return NULL;
  }

  sqlite3_stmt *stmt;
  const char *tail;
  int rc = sqlite3_prepare_v2(db, sql_stmt_text, (int)sql_length + 1, &stmt, &tail);
  if (rc != SQLITE_OK) {
    char buf[80];
    snprintf(buf, sizeof(buf), "SQL statement text is not valid SQL (%d)", rc);
    sqlite3_result_error(context, buf, -1);
    sqlite3_result_error_code(context, rc);
    return NULL;
  }

  if (stmt == NULL)
  {
    sqlite3_result_error(context, "SQL statement preparation failed", -1);
    sqlite3_result_error_code(context, SQLITE_FORMAT);
    return NULL;
  }

  return stmt; // Caller owns and must finalize the sqlite3_stmt.
}

/*
 * Implementation of the "sqlite3_stmt_readonly()" SQL function. This wraps the
 * C API call of the same name <https://sqlite.org/c3ref/stmt_readonly.html>.
 */
static void sqlite3_stmt_readonly_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 1);
  sqlite3_stmt *stmt = get_sql_statement(context, argv[0]);
  if (stmt == NULL)
  {
    // Error code and message already set by get_sql_statement().
    return;
  }

  // As stmt is not NULL, we own and must finalize it.

  int result = sqlite3_stmt_readonly(stmt);
  sqlite3_result_int(context, result);

  int rc = sqlite3_finalize(stmt);
  if (rc != SQLITE_OK)
  {
    sqlite3_result_error(context, "sqlite3_finalize failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }
}

/*
 * Implementation of the "sqlite3_normalized_sql()" SQL function. This wraps the
 * C API call of the same name <https://sqlite.org/c3ref/stmt_readonly.html>.
 */
static void sqlite3_normalized_sql_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 1);
  if (sqlite3_normalized_sql == NULL)
  {
    sqlite3_result_error(context, "sqlite3_normalized_sql not compiled in", -1);
    sqlite3_result_error_code(context, SQLITE_NOTFOUND);
    return;
    
  }

  sqlite3_stmt *stmt = get_sql_statement(context, argv[0]);
  if (stmt == NULL)
  {
    // Error code and message already set by get_sql_statement().
    return;
  }

  // As stmt is not NULL, we own and must finalize it.

  const char * normalized_sql = sqlite3_normalized_sql(stmt);
  if (normalized_sql == NULL)
  {
    sqlite3_finalize(stmt);
    sqlite3_result_error(context, "sqlite3_normalized_sql failed", -1);
    sqlite3_result_error_code(context, SQLITE_FORMAT);
    return;
  }
  else
  {
    sqlite3_result_text(context, normalized_sql, -1, NULL);
  }

  int rc = sqlite3_finalize(stmt);
  if (rc != SQLITE_OK)
  {
    sqlite3_result_error(context, "sqlite3_finalize failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }
}

static bool get_schema_name(sqlite3_context *context, sqlite3_value *arg,
  const char ** result_out)
{
  sqlite3 * db = sqlite3_context_db_handle(context);
  int schema_name_length = sqlite3_value_bytes(arg);
  int max_length = sqlite3_limit(db, SQLITE_LIMIT_SQL_LENGTH, -1);
  if (schema_name_length > max_length)
  {
    sqlite3_result_error(context, "Schema name is too long", -1);
    sqlite3_result_error_toobig(context);
    return false;
  }

  const char * schema_name = (const char *)sqlite3_value_text(arg);
  // A NULL pointer can be used in place of "main" to refer to the main
  // database file.
  if (schema_name != NULL)
  {
    if (strlen(schema_name) != (size_t)schema_name_length)
    {
      sqlite3_result_error(context, "Schema name contains a NUL byte", -1);
      sqlite3_result_error_code(context, SQLITE_MISMATCH);
      return false;
    }
  }

  *result_out = schema_name;
  return true;
}

/*
 * Implementation of the "sqlite3_lock_state()" SQL function. This is not based
 * on any official SQLite user API. It calls the VFS method
 * xCheckReservedLock(), which checks whether any database connection, either in
 * this process or in some other process, is holding a RESERVED, PENDING, or
 * EXCLUSIVE lock on the file. It returns true if such a lock exists and false
 * otherwise. 
 * <https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntllockstate>
 */
static void sqlite3_lock_state_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 1);
  sqlite3 * db = sqlite3_context_db_handle(context);
  const char * schema_name;
  if (!get_schema_name(context, argv[0], &schema_name))
  {
    // Error code and message already set by get_schema_name().
    return;
  }

  int result;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_LOCKSTATE, &result);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_LOCKSTATE failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  sqlite3_result_int(context, result);
}

/*
 * Implementation of the "sqlite3_check_reserved_lock()" SQL function. This is
 * not based on any official SQLite user API. It calls the VFS method
 * xCheckReservedLock(), which checks whether any database connection, either in
 * this process or in some other process, is holding a RESERVED, PENDING, or
 * EXCLUSIVE lock on the file. It returns true if such a lock exists and false
 * otherwise. <https://sqlite.org/c3ref/io_methods.html>
 */
static void sqlite3_check_reserved_lock_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 1);
  const char * schema_name;
  if (!get_schema_name(context, argv[0], &schema_name))
  {
    // Error code and message already set by get_schema_name().
    return;
  }

  sqlite3 * db = sqlite3_context_db_handle(context);
  sqlite3_file * pFile = NULL;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_FILE_POINTER, &pFile);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  if (pFile == NULL)
  {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER returned NULL", -1);
    return;
  }

  /*
  sqlite3_vfs* pVfs = NULL;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_VFS_POINTER, &pVfs);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_VFS_POINTER failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  if (pVfs == NULL)
  {
    sqlite3_result_error(context, "SQLITE_FCNTL_VFS_POINTER returned NULL", -1);
    return;
  }
  */

  int result;
  rc = pFile->pMethods->xCheckReservedLock(pFile, &result);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "xCheckReservedLock failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  sqlite3_result_int(context, result);
}

/*
 * Implementation of the "sqlite3_external_reader()" SQL function.
 *
 * This uses the sqlite3_file_control function with the
 * SQLITE_FCNTL_EXTERNAL_READER argument to check for external locks (in other
 * processes) on this database file.
 * <https://sqlite.org/c3ref/io_methods.html>
 */
static void sqlite3_external_reader_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 2);
  const char * schema_name;
  if (!get_schema_name(context, argv[0], &schema_name))
  {
    // Error code and message already set by get_schema_name().
    return;
  }

  sqlite3 * db = sqlite3_context_db_handle(context);
  int external_reader;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_EXTERNAL_READER,
    &external_reader);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_EXTERNAL_READER failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  sqlite3_result_int(context, external_reader);
}

/*
 * Implementation of the "sqlite3_xlock()" SQL function.
 *
 * This is not based on any official SQLite user API. It calls the VFS method
 * xLock(), which upgrades the database file lock. In other words, xLock() moves
 * the database file lock in the direction NONE toward EXCLUSIVE. The argument
 * to xLock() is always one of SHARED, RESERVED, PENDING, or EXCLUSIVE, never
 * SQLITE_LOCK_NONE. If the database file lock is already at or above the
 * requested lock, then the call to xLock() is a no-op.
 * <https://sqlite.org/c3ref/io_methods.html>
 *
 * This is only used in tests, it's probably not a good idea to call it on a
 * real database!
 */
static void sqlite3_xlock_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 2);
  const char * schema_name;
  if (!get_schema_name(context, argv[0], &schema_name))
  {
    // Error code and message already set by get_schema_name().
    return;
  }

  sqlite3 * db = sqlite3_context_db_handle(context);
  sqlite3_file * pFile = NULL;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_FILE_POINTER, &pFile);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  if (pFile == NULL)
  {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER returned NULL", -1);
    return;
  }

  int eFileLock = sqlite3_value_int(argv[1]);
  rc = pFile->pMethods->xLock(pFile, eFileLock);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "xLock failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }
}

/*
 * Implementation of the "sqlite3_xunlock()" SQL function.
 *
 * This is not based on any official SQLite user API. It calls the VFS method
 * xUnlock(), which downgrades the database file lock to either SHARED or NONE.
 * If the lock is already at or below the requested lock state, then the call to
 * xUnlock() is a no-op.
 * <https://sqlite.org/c3ref/io_methods.html>
 *
 * This is only used in tests, it's probably not a good idea to call it on a
 * real database!
 */
static void sqlite3_xunlock_func(sqlite3_context *context, int argc,
  sqlite3_value **argv)
{
  assert(argc == 2);
  const char * schema_name;
  if (!get_schema_name(context, argv[0], &schema_name))
  {
    // Error code and message already set by get_schema_name().
    return;
  }

  sqlite3 * db = sqlite3_context_db_handle(context);
  sqlite3_file * pFile = NULL;
  int rc = sqlite3_file_control(db, schema_name, SQLITE_FCNTL_FILE_POINTER, &pFile);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }

  if (pFile == NULL)
  {
    sqlite3_result_error(context, "SQLITE_FCNTL_FILE_POINTER returned NULL", -1);
    return;
  }

  int eFileLock = sqlite3_value_int(argv[1]);
  rc = pFile->pMethods->xUnlock(pFile, eFileLock);
  if (rc != SQLITE_OK) {
    sqlite3_result_error(context, "xLock failed", -1);
    sqlite3_result_error_code(context, rc);
    return;
  }
}
/*
 * Initializer (entry point) for this SQLite extension. This has the default
 * name that SQLite looks for, based on the dynamically loaded library filename:
 * sqlite3_sqliteextension.so -> sqliteextension -> sqlite3_sqliteextension_init.
 * This means it should be possible to load it without specifying the name of
 * this entry point function explicitly.
 */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sqliteextension_init( // default name is sqlite3_extension_init
  sqlite3 *db,  char **pzErrMsg, const sqlite3_api_routines *pApi)
{
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  /* insert code to initialize your extension here */
  rc = sqlite3_create_function(db, "sqlite3_txn_state", 1, 
    SQLITE_UTF8 | SQLITE_DIRECTONLY,
    0, sqlite3_txn_state_func, NULL, NULL);
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_stmt_readonly", 1, 
      SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_DIRECTONLY | SQLITE_INNOCUOUS,
      NULL, sqlite3_stmt_readonly_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_normalized_sql", 1, 
      SQLITE_UTF8 | SQLITE_DIRECTONLY | SQLITE_INNOCUOUS,
      NULL, sqlite3_normalized_sql_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_lock_state", 1, 
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, sqlite3_lock_state_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_check_reserved_lock", 1, 
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, sqlite3_check_reserved_lock_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_external_reader", 1, 
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, sqlite3_external_reader_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_xlock", 2,
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, sqlite3_xlock_func, NULL, NULL);
  }
  if (rc == SQLITE_OK)
  {
    rc = sqlite3_create_function(db, "sqlite3_xunlock", 2,
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, sqlite3_xunlock_func, NULL, NULL);
  }
  
  return rc;
}
