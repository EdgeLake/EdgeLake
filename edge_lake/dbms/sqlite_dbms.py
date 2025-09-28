"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import sys
import sqlite3
from sqlite3 import Error

from edge_lake.dbms.database import sql_storage
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
from edge_lake.generic.params import get_path_separator
import edge_lake.generic.interpreter as interpreter

# Map the synchronous level to a more understandable format
sync_map_ = {
    0: "off",
    1: "normal (critical points)",
    2: "full (after each write)",
    3: "extra (Extra syncs)"
}


# =======================================================================================================================
# SQLITE Instance
# =======================================================================================================================
class SQLITE(sql_storage):

    def __init__(self, in_ram):
        super().__init__()
        self.dbms_name = ""
        self.path = ""  # path to the data files
        self.in_ram = in_ram  # True for an in memory dbms
        self.connect_name = ""  # the name including the path to the location of the database data
        self.first_conn = None
        self.engine_name = "sqlite"
        self.autocommit = True
        self.single_insert = True   # The JSON are mapped to a single insert. False maps the JSON to multiple inserts

    # =======================================================================================================================
    #  Return the special configuration for "get databases" command
    # =======================================================================================================================
    def get_config(self, status):

        if self.autocommit:
            config_str = "Autocommit On"
        else:
            config_str = "Autocommit Off"

        if self.in_ram:
            config_str += ", RAM"

        synchronous_status = -1
        cursor = self.get_cursor(status)
        if cursor:
            if self.execute_sql_stmt(status, cursor, "PRAGMA synchronous;"):
                synchronous_status = cursor.fetchone()[0]
            self.close_cursor(status, cursor)
        sync_stat = sync_map_.get(synchronous_status, "Unknown")

        config_str += f", Fsync {sync_stat}"

        return config_str
    # =======================================================================================================================
    #  Return True if the Cusror is self contained - it does not need any info from AnyLog
    # =======================================================================================================================
    def is_self_contained_curosr(self):
        return True  # The cursor implementation does not need info

    # =======================================================================================================================
    #  get IP and Port of the DBMS
    # =======================================================================================================================
    def get_ip_port(self):
        return "Local"

    # =======================================================================================================================
    #  Return the storage type
    # =======================================================================================================================
    def get_storage_type(self):

        if self.in_ram:
            return "MEMORY"

        return self.connect_name

    # =======================================================================================================================
    #  SETUP Calls - These are issued when the database is created
    # =======================================================================================================================
    def exec_setup_stmt(self, status):
        return True

    # =======================================================================================================================
    # When row are retrieved, need to call query_row_by_row()
    # =======================================================================================================================
    def is_row_by_row(self):
        return True

    # =======================================================================================================================
    # In SQLite, each thread needs a connection
    # =======================================================================================================================
    def is_thread_connection(self):
        return True

    # =======================================================================================================================
    # Flag if join info is supported
    # =======================================================================================================================
    def is_suport_join(self):
        return False

    # =======================================================================================================================
    # True is returned if the database process the entire where condition
    # =======================================================================================================================
    def is_process_where(self):
        return True

    # =======================================================================================================================
    #  Returns True if the engine API manage pool of connections
    # =======================================================================================================================
    def with_connection_pool(self):
        return False

    # =======================================================================================================================
    # Return the database size in BYTES
    # =======================================================================================================================
    def get_dbms_size(self, status):
        if self.in_ram:
            size = 0  # All in Ram
        else:
            size = utils_io.get_file_size(status, self.connect_name)
            if size == -1:
                size = 0  # The database was not yet created on disk

        return str(size)

    # ==================================================================
    # Create a database
    # If memory flag is ON, the special name :memory: creates the dbms in RAM
    # Default location is for the database is used, unless a path is specified
    # ==================================================================
    def connect_to_db(self, status, user, passwd, host, port, dbn, conditions):

        self.dbms_name, self.connect_name, self.path = get_dbms_connect_info(dbn, self.in_ram)

        if conditions:
            self.autocommit = interpreter.get_one_value_or_default(conditions, "autocommit", True)

        ret_val = self.exec_pragma(status,
                                   "PRAGMA auto_vacuum = INCREMENTAL;")  # This call allows to reclaim space when a table is dropped - "PRAGMA incremental_vacuum(0);"

        if ret_val and self.in_ram:
            # keep a ptr to first connection as with memory dbms -
            # data is deleted and memory is reclaimed when the last connection closes.
            # memory dbms info - https://www.sqlite.org/uri.html and https://www.sqlite.org/sharedcache.html
            self.first_conn = self.db_connect_thread(status)


        return ret_val

    # ==================================================================
    # In SQLite, each thread needs a seperate connection
    # https://www.sqlite.org/inmemorydb.html
    # ==================================================================
    def db_connect_thread(self, status):

        try:
            if self.in_ram:
                # memory dbms info - https://www.sqlite.org/uri.html and https://www.sqlite.org/sharedcache.html
                # URI documentation
                conn = sqlite3.connect(self.connect_name, uri=True, timeout=100000, check_same_thread=False)   # Keep long timeout because a lot of conflicts if autocommit is changed to False
            else:
                conn = sqlite3.connect(self.connect_name, timeout=10, check_same_thread=False) # how long the connection should wait for a lock until raising an exception. The default for the timeout parameter is 5.0 (five seconds).

            conn.execute("PRAGMA busy_timeout = 5000;") # change the busy timeout after the connection has been established:
            #conn.execute("PRAGMA journal_mode=WAL;")

            if self.autocommit:
                # Use autocommit
                conn.isolation_level = None  # None for autocommit mode - https://docs.python.org/2/library/sqlite3.html
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error("Failed to connect to SQLITE to create database '%s': '%s'" % (self.connect_name, str(value)))
            conn = None

        return conn

    # ==================================================================
    # Close thread connection
    # ==================================================================
    def db_close_thread_connect(self, status, conn):

        try:
            conn.close()
        except:
            status.add_error("SQLite failed to close connection with database %s" % self.connect_name)

    # ==================================================================
    # Get Cursor
    # SQLite objects created in a thread can only be used in that same thread.
    # Therefore, dbms connection is maintained with cursor
    # ==================================================================
    def get_cursor(self, status):

        conn = self.db_connect_thread(status)  # this is done as connection is per thread

        if conn:
            try:
                db_cursor = conn.cursor()
            except Error as e:
                db_cursor = None

            if not db_cursor:
                self.db_close_thread_connect(conn)

        else:
            db_cursor = None

        return db_cursor

    # =======================================
    # Reclaim database space
    # PRAGMA schema.incremental_vacuum(N);
    #
    # From http://man.hubwiz.com/docset/SQLite.docset/Contents/Resources/Documents/sqlite/pragma.html#pragma_incremental_vacuum
    # The incremental_vacuum pragma causes up to N pages to be removed from the freelist.
    # The database file is truncated by the same amount.
    # The incremental_vacuum pragma has no effect if the database is not in auto_vacuum=incremental mode
    # or if there are no pages on the freelist. If there are fewer than N pages on the freelist,
    # or if N is less than 1, or if N is omitted entirely, then the entire freelist is cleared.
    # ======================================
    def reclaim_space(self, status: process_status):

        #  incremental auto_vacuum in SQLite so that the database file is prepared
        #  but the freelist pages must be manually removed from the database file
        #  with an additional PRAGMA statement"

        # Called when database is opened - "PRAGMA auto_vacuum = INCREMENTAL;"
        # Called when space is reclaimed - "PRAGMA incremental_vacuum(0);"

        ret_val = self.exec_pragma(status, "PRAGMA incremental_vacuum(0);")

        return ret_val

    # ======================================
    # PRAGMA is a sqlite proprietry call
    # ======================================
    def exec_pragma(self, status: process_status, pragma_text: str):

        cursor = self.get_cursor(status)
        ret_val = False
        if cursor:
            if self.execute_sql_stmt(status, cursor, pragma_text):
                ret_val = True
            self.close_cursor(status, cursor)

        return ret_val

    # ==================================================================
    # Close connection to database
    # ==================================================================
    def close_connection(self, status: process_status, db_connect):
        # with SQLite, connection is closed when cursor is closed
        if self.first_conn:
            # with memory dbms - self.first_conn keeps a connection open
            # data is deleted and memory is reclaimed when the last connection closes.
            self.db_close_thread_connect(status, self.first_conn)
            self.first_conn = None

        return True

    # ==================================================================
    # Close Cursor
    # ==================================================================
    def close_cursor(self, status, db_cursor):

        try:
            db_cursor.close()
        except:
            pass

        self.db_close_thread_connect(status, db_cursor.connection)

    # ==================================================================
    # Commit
    # ==================================================================blockchain show operator
    def commit(self, status, db_cursor):
        try:
            db_cursor.connection.commit()
        except:
            error_msg = "SQLite failed to commit with database %s" % self.connect_name
            status.add_keep_error(error_msg)

    # ==================================================================
    # Rollback
    # ==================================================================blockchain show operator
    def rollback(self, status, db_cursor):
        try:
            db_cursor.connection.rollback()
        except:
            error_msg = "SQLite failed to rollback with database %s" % self.connect_name
            status.add_keep_error(error_msg)

    # ==================================================================
    # Get number of rows affected
    # ==================================================================
    def get_rows_affected(self, status, db_cursor):
        try:
            rows_count = db_cursor.rowcount
        except:
            status.add_error("Request for DBMS rows count with invalid cursor")
            rows_count = 0
        return rows_count

    # ==================================================================
    # Execute SQL from a buffer
    # ==================================================================
    def process_multi_insert_buff(self, status, dbms_name, table_name, db_cursor, insert_buffer):

        ret_val = self.execute_sql_stmt(status, db_cursor, insert_buffer)

        return ret_val

    # ==================================================================
    # Execute SQL as a stmts
    # ==================================================================
    def execute_sql_file(self, status: process_status, dbms_name, table_name, db_cursor, sql_file):

        ret_val = True

        file_path = os.path.expanduser(os.path.expandvars(sql_file))
        try:
            f = open(file_path, 'r')
        except:
            error_msg = "Error: Failed to open Insert file: '%s'" % sql_file
            status.add_keep_error(error_msg)
            return [False, 0]

        if self.single_insert:
            try:
                entry = f.read()
            except:
                error_msg = "Error: Failed to read from Insert file: '%s'" % sql_file
                status.add_keep_error(error_msg)
                return [False, 0]
            else:
                ret_val = self.execute_sql_stmt(status, db_cursor, entry)
                rows_counter = self.get_rows_affected(status, db_cursor)

        else:
            # Add row after row
            for rows_counter, entry in enumerate(f):
                if not self.execute_sql_stmt(status, db_cursor, entry):
                    ret_val = False
                    break
            if ret_val:
                rows_counter += 1
            else:
                rows_counter = 0

        if ret_val == False:
            error_msg = "Error executing SQL from file: %s" % sql_file
            status.add_error(error_msg)
            status.keep_error(error_msg)
            rows_counter = 0
        else:
            self.commit(status, db_cursor)

        try:
            f.close()
        except IOError as e:
            error_msg = "Error: Failed to close Insert file: '%s'" % sql_file
            status.add_keep_error(error_msg)

        return [ret_val, rows_counter]

    # ==================================================================
    # Execute SQL statement
    # ==================================================================blockchain show operator
    def execute_sql_stmt(self, status: process_status, db_cursor, sql_stmt: str, ignore_error = False):

        ret_val = True

        try:
            db_cursor.execute(sql_stmt)
        except sqlite3.DataError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.InternalError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.IntegrityError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.OperationalError as e:
            if sql_stmt.endswith("set unlogged;"):
                pass  # "set unlogged" not supported
            else:
                error_msg = str(e)
                ret_val = False
        except sqlite3.NotSupportedError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.ProgrammingError as e:
            error_msg = str(e)
            ret_val = False
        except (Exception, sqlite3.Error) as e:
            error_msg = str(e)
            ret_val = False
        except:
            error_msg = "Unknown error"
            ret_val = False
        else:
            error_msg = ""

        if utils_sql.is_trace_sql():
            utils_sql.trace_sql("SQLite", sql_stmt, ret_val, ignore_error, error_msg)

        if ret_val == False:
            if ignore_error:
                ret_val = True
            else:
                error_msg = "Error executing SQL:\r\n%s\r\n%s" % (sql_stmt, error_msg)
                status.add_error(error_msg)
                status.keep_error(error_msg)

        return ret_val

    # ==================================================================
    # Fetch results
    # if fetch_size is 0, execute fetchall()
    # Return String
    # ==================================================================
    def fetch_rows(self, status: process_status, db_cursor, output_prefix, fetch_size,
                   title_list, type_list):

        ret_val, output = self.fetch_list(status, db_cursor, fetch_size)

        output_len = len(output)  # the size of the list with the result set
        if ret_val and output_len:
            string_data = utils_sql.format_db_rows(status, db_cursor, output_prefix, output, title_list, type_list)
        else:
            string_data = ""

        reply_list = [ret_val, string_data]
        return reply_list

    # ==================================================================
    # Fetch results for execute_sql_without_fetch
    # if fetch_size is 0, execute fetchall()
    # Return List
    # ==================================================================
    def fetch_list(self, status: process_status, db_cursor, fetch_size):

        ret_val = True
        try:
            if fetch_size:
                output = db_cursor.fetchmany(fetch_size)  # fetchmany is a list object
            else:
                output = db_cursor.fetchall()

        except sqlite3.DataError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.InternalError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.IntegrityError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.OperationalError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.NotSupportedError as e:
            error_msg = str(e)
            ret_val = False
        except sqlite3.ProgrammingError as e:
            error_msg = str(e)
            ret_val = False
        except (Exception, sqlite3.Error) as e:
            error_msg = str(e)
            ret_val = False
        except:
            error_msg = "Unknown error"
            ret_val = False

        if ret_val == False:
            output = ""
            error_msg = "Unable to fetch query results:\n%s" % error_msg
            status.add_error(error_msg)
            status.keep_error(error_msg)

        reply_list = [ret_val, output]
        return reply_list

    # =======================================
    # A DBMS that takes PostgreSQL calls returns True
    # A DBMS like PI that considers the generic calls returns False
    # ======================================
    def is_default_sql_command(self):
        return True

    # =======================================
    # Get database info
    # Given a dbms name, get the list of tables
    # ======================================
    def get_database_tables(self, status: process_status, dbms_name: str):

        sql_string = "select name from sqlite_master where type='table' and name not like 'sqlite_%';"

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_string):  # get column info
                ret_val, output = self.fetch_list(status, cursor, 0)  # returns an array with info on the columns
            else:
                ret_val = False
            self.close_cursor(status, cursor)
        else:
            ret_val = False

        data_string = "{\"Structure.Tables." + dbms_name + "\":["

        if ret_val:

            for id, entry in enumerate(output):

                table_name = ",{\"table_name\":\"" + str(entry)[2:-3] + "\"}"
                if id:
                    data_string += table_name
                else:
                    data_string += table_name[1:]  # ignore comma

        else:
            data_string = ""

        data_string += "]}"

        reply_list = [ret_val, data_string]
        return reply_list

    # =======================================
    # Given a dbms name and a table name, get the list of partitions
    # ======================================
    def get_table_partitions(self, status: process_status, dbms_name: str, table_name: str):

        if table_name == '*':
            # All tables of the database
            sql_string = "select name from sqlite_master where type='table' and name like 'par_%s' order by name;" % ("%")
        else:
            # specific table
            sql_string = "select name from sqlite_master where type='table' and name like 'par_%s_%s' order by name;" % (table_name, "%")

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_string):  # get column info
                ret_val, output = self.fetch_list(status, cursor, 0)  # returns an array with info on the columns
            else:
                ret_val = False
            self.close_cursor(status, cursor)
        else:
            ret_val = False

        data_string = "{\"Partitions." + dbms_name + "." + table_name + "\":["

        if ret_val:

            for id, entry in enumerate(output):

                table_name = ",{\"par_name\":\"" + str(entry)[2:-3] + "\"}"
                if id:
                    data_string += table_name
                else:
                    data_string += table_name[1:]  # ignore comma

        else:
            data_string = ""

        data_string += "]}"

        reply_list = [ret_val, data_string]
        return reply_list

    # =======================================
    # Get the list of TSD tables
    # ======================================
    def get_tsd_tables_list(self, status):

        sql_string = "select name from sqlite_master where type='table' and name like 'tsd_%' order by name;"

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_string):  # get column info
                ret_val, output = self.fetch_list(status, cursor, 0)  # returns an array with info on the columns
            else:
                ret_val = False
            self.close_cursor(status, cursor)
        else:
            ret_val = False

        data_string = "{\"Tables.almgm.tsd\":["

        if ret_val:

            for id, entry in enumerate(output):

                table_name = ",{\"table_name\":\"" + str(entry)[2:-3] + "\"}"
                if id:
                    data_string += table_name
                else:
                    data_string += table_name[1:]  # ignore comma

        else:
            data_string = ""

        data_string += "]}"

        reply_list = [ret_val, data_string]
        return reply_list

    # =======================================
    # Get table info
    # Return a list of columns and their data types
    # ======================================
    def get_column_info(self, status: process_status, dbms_name: str, table_name: str):
        sql_string = "PRAGMA TABLE_INFO(%s)" % table_name

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_string):  # get column info
                ret_val, output = self.fetch_list(status, cursor, 0)  # returns an array with info on the columns
            else:
                output = None
                ret_val = False
            self.close_cursor(status, cursor)
        else:
            output = None
            ret_val = False

        reply_list =  [ret_val, output]
        return reply_list

    # =======================================
    # Get table info
    # Given a table name, generate a list of column corresponding to table
    # ======================================
    def get_table_info(self, status: process_status, dbms_name: str, table_name: str, info_type: str):

        sql_string = "PRAGMA TABLE_INFO(%s)" % table_name

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_string):  # get column info
                ret_val, output = self.fetch_list(status, cursor, 0)  # returns an array with info on the columns
            else:
                ret_val = False
            self.close_cursor(status, cursor)
        else:
            ret_val = False

        if ret_val and len(output):

            data_string = "{\"Structure." + dbms_name + "." + table_name + "\":["

            for id, entry in enumerate(output):
                # go over the columns
                if info_type == "columns":
                    # column names and types
                    column_name_type = ",{\"column_name\":\"" + entry[1] + "\",\"data_type\":\"" + entry[2] + "\"}"
                    if id:
                        data_string += column_name_type.lower()
                    else:
                        data_string += column_name_type[1:].lower()  # ignore comma
                else:
                    # only column names
                    column_name = ",{\"column_name\":\"" + entry[1].lower() + "\"}"
                    if id:
                        data_string += column_name.lower()
                    else:
                        data_string += column_name[1:].lower()  # ignore comma

            data_string += "]}"
        else:
            data_string = ""

        reply_list = [ret_val, data_string]
        return reply_list

    # ==================================================================
    # Commands that needs to be executed when a temprary table is created
    # ==================================================================
    def commands_for_temp_table(self, table_name):
        return ""

    # ==================================================================
    # Fetch all rows - return as a json string
    # ==================================================================
    def select_as_json(self, status: process_status, output_prefix: str, sql_stmt: str, title_list: list):

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_stmt):
                ret_val, string_data = self.fetch_rows(status, cursor, output_prefix, 0, title_list, None)  # 0 is fetchall()
            else:
                ret_val = False
                string_data = None
            self.close_cursor(status, cursor)
        else:
            ret_val = False
            string_data = None

        reply_list = [ret_val, string_data]
        return reply_list

    # ==================================================================
    # Fetch all rows  - return as a list
    # ==================================================================
    def select_as_list(self, status: process_status, sql_stmt: str):

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_stmt):
                ret_val, list_data = self.fetch_list(status, cursor, 0)
            else:
                ret_val = False
                list_data = None
            self.close_cursor(status, cursor)
        else:
            ret_val = False
            list_data = None

        reply_list =  [ret_val, list_data]
        return reply_list


    # ==================================================================
    # Return True if a table exists
    # ==================================================================
    def is_table_exists(self, status: process_status, table_name: str):

        sql_string = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '%s'" % table_name

        ret_val, data_string = self.select_as_json(status, "", sql_string, [])

        if ret_val:
            if len(data_string):
                ret_val = True
            else:
                ret_val = False

        return ret_val

    # =======================================================================================================================
    # make changes that are specific to the database
    # =======================================================================================================================
    def modify_sql_create(self, sql_command):

        # REPLACE " SERIAL PRIMARY KEY," --> " INTEGER PRIMARY KEY AUTOINCREMENET,"
        update_sql = sql_command.lower()

        updated_sql = update_sql.replace(" serial primary key,", " integer primary key autoincrement,", 1)

        updated_sql = updated_sql.replace(" timestamp not null default now()",
                                          " timestamp not null default current_timestamp")

        updated_sql = updated_sql.replace(" date not null default now()",
                                          " date not null default (date('now'))")

        updated_sql = updated_sql.replace(" time not null default now()",
                                          " time not null default (time('now'))")

        updated_sql = updated_sql.replace(" serial primary key ",
                                          " integer primary key autoincrement ")

        updated_sql = updated_sql.replace(" bigint ",
                                          " integer ")  # SQLite does not have a separate BIGINT data type


        #  TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
        # updated_sql = updated_sql.replace(" NOW()", " (datetime('now','localtime'))")

        # SQLite needs every command to be executed seperately. Create index is broken from the create string.
        commands_array = [i for i in range(len(updated_sql)) if updated_sql[i] == ';']  # get all the subcommands

        reply_list = [commands_array, updated_sql]  # return all offset to commamds

        return reply_list

    # =======================================================================================================================
    # make changes that are specific to the database
    # =======================================================================================================================
    def modify_sql_drop(self, sql_command):

        commands_array = [len(sql_command) - 1]  # No changes are needed

        reply_list = [commands_array, sql_command]  # return all offset to commands
        return reply_list

    # =======================================================================================================================
    # make changes that are specific to the database
    # =======================================================================================================================
    def modify_sql_select(self, sql_command):

        sql_stmt = sql_command[4:]  # Remove the type of change needed
        change_id = sql_command[0:4]  # <00> = test for all

        if change_id == '<01>' or change_id == '<00':

            # Change the text between <TIMESTAMP_START> and <TIMESTAMP_END>
            end_pos = len(sql_stmt) - 15
            while 1:
                if end_pos < 33:        # greater than len(<TIMESTAMP_START>) + len(<TIMESTAMP_END>)
                    break
                index_start = sql_stmt.rfind("<TIMESTAMP_START>", 0, end_pos)
                if index_start == -1:
                    break       # No changes
                index_end = sql_stmt.find("<TIMESTAMP_END>", index_start + 17)
                if index_end == -1:
                    break
                # Remove <TIMESTAMP_START> and <TIMESTAMP_END>
                sql_stmt = (sql_stmt[:index_start] + sql_stmt[index_start + 17:index_end] + sql_stmt[index_end+15:])     # remove the formatting info
                end_pos = index_start - 15

        return sql_stmt

    # =======================================
    # Map rows to insert statements
    # ======================================
    def get_insert_rows(self, status: process_status, dbms_name: str, table_name: str, insert_size: int, column_names: list, insert_rows: list):

        if self.single_insert:
            # One insert command
            inserts = self.get_single_insert(process_status, dbms_name, table_name, insert_size, column_names, insert_rows)
        else:
            # Multiple commands
            inserts = self.get_multiple_inserts(process_status, dbms_name, table_name, insert_size, column_names, insert_rows)
        return inserts
    # =======================================
    # Get a single insert for all new entries
    # ======================================
    def get_single_insert(self, status: process_status, dbms_name: str, table_name: str, insert_size: int,
                          column_names: list, insert_rows: list):

        # Generate column names string
        column_names_str = ", ".join(entry[1] for entry in column_names)
        insert_statements = []

        for row in insert_rows:
            if not row:
                continue

            # Generate values string
            columns_string = ", ".join("NULL" if (isinstance(col_val, str) and col_val == "DEFAULT") else str(col_val) for col_val in row)

            insert_statements.append(f"({columns_string})")

        return f"INSERT INTO {table_name} ({column_names_str}) VALUES " + ",\n".join(insert_statements) + ';'

    # =======================================
    # Map rows to insert statements
    # Adding column names and removing default values
    # ======================================
    def get_multiple_inserts(self, status: process_status, dbms_name: str, table_name: str, insert_size: int,
                             column_names: list, insert_rows: list):
        insert_statements = []
        counter_columns = len(column_names)

        for column_values in insert_rows:
            if not column_values:
                continue

            if counter_columns < len(column_values):
                status.add_error(
                    f"Table '{dbms_name}.{table_name}' is declared with {counter_columns} columns and row inserted has {len(column_values)} columns")
                return ""

            names_list = []
            values_list = []

            for index, value in enumerate(column_values):
                if value != "DEFAULT":
                    # Add column name and value
                    names_list.append(column_names[index][1])
                    values_list.append(value)

            insert_stmt = f"INSERT INTO {table_name} ({','.join(names_list)}) VALUES ({','.join(values_list)});"
            insert_statements.append(insert_stmt)

        return "\n".join(insert_statements)

# =======================================================================================================================
# Database name can include path - seperate the database name from the path
# If in_ran is true - dbms is to be created in memory
# =======================================================================================================================
def get_dbms_connect_info(dbn, in_ram):
    dbms_path = ""
    if dbn.find('/') != -1:
        path_char = '/'
    elif dbn.find('\\') != -1:
        path_char = '\\'
    else:
        dbms_name = dbn
        path_char = ''

    if path_char:
        dbms_name = dbn.rsplit(path_char, 1)[-1]
        dbms_path = dbn[:len(dbn) - len(dbms_name)]
        dbms_path = utils_io.get_path(dbms_path)

    if in_ram:
        connect_name = "file:" + "%s?mode=memory&cache=shared" % dbms_name  # this name creates the dbms n RAM
    else:
        if dbms_path == "":
            # place in default dir
            dbms_path = params.get_param("dbms_dir")
            if dbms_path:
                dbms_path = utils_io.get_path(dbms_path) + get_path_separator()

        if dbms_name.find(".") == -1:
            # add .dbms
            connect_name = dbms_path + dbms_name + ".dbms"
        else:
            connect_name = dbms_path + dbms_name

    reply_list = [dbms_name, connect_name, dbms_path]
    return reply_list
