'''
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
'''
is_supported = True
try:
    import psycopg2
    from psycopg2 import pool
except:
    is_supported = False

import os
import sys

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_sql as utils_sql


class PSQL:
    def __init__(self):
        self.conn_pool = None
        self.dbms_name = ""
        self.ip_port = ""
        self.engine_name = "psql"
        self.autocommit = True

    def get_engine_name(self):
        return self.engine_name

    def get_dbms_name(self):
        return self.dbms_name

    def retrieve_files(self, *args):
        return process_status.No_dbms_engine_support

    def remove_multiple_files(self, *args):
        return process_status.No_dbms_engine_support

    def remove_file(self, *args):
        return process_status.No_dbms_engine_support

    def get_file_list(self, *args):
        return None

    def count_files(self, *args):
        return -1

    def store_file(self, *args):
        return False

    # =======================================================================================================================
    #  Return True if the Cusror is self contained - it does not need any info from AnyLog
    # =======================================================================================================================
    def is_self_contained_curosr(self):
        return True  # The cursor implementation does not need info

    # =======================================================================================================================
    #  get IP and Port of the DBMS
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip_port

    # =======================================================================================================================
    #  Return True for SQL Storage
    # =======================================================================================================================
    def is_sql_storage(self):
        return True
    # =======================================================================================================================
    #  Return the storage type
    # =======================================================================================================================
    def get_storage_type(self):
        return "Persistent"

    # =======================================================================================================================
    #  SETUP Calls - These are issued when the database is created
    # =======================================================================================================================
    def exec_setup_stmt(self, status):

        ret_val = False

        stmt = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"';

        db_cursor = self.get_cursor(status)
        if db_cursor:
            ret_val = self.execute_sql_stmt(status, db_cursor, stmt)

        self.close_cursor(status, db_cursor)

        return ret_val

    # =======================================================================================================================
    # make changes that are specific to the database
    # =======================================================================================================================
    def modify_sql_create(self, sql_command):

        sql_create = sql_command.replace(" double,", " double precision,")  # add "precision" if needed

        sql_create = sql_create.replace(" double)", " double precision)")  # double is last on the create stmt


        commands_array = [len(sql_create) - 1]  # No changes are needed

        reply_list = [commands_array, sql_create]  # return all offset to commamds
        return reply_list

    # =======================================================================================================================
    # make changes that are specific to the database
    # CASCADE - Automatically drop objects that depend on the table (such as views).
    # =======================================================================================================================
    def modify_sql_drop(self, sql_command):

        if sql_command[-1] == ';':
            sql_command = sql_command[:-1] + " cascade;"
        else:
            sql_command += " cascade;"


        commands_array = [len(sql_command) - 1]  # No changes are needed

        reply_list = [commands_array, sql_command]  # return all offset to commamds
        return reply_list

    # =======================================================================================================================
    # make changes that are specific to the database
    # Example: "<01>select file_id, <TIMESTAMP_START>file_time<TIMESTAMP_END> from %s where file_time <= '%s' order by file_id desc limit 1"
    # =======================================================================================================================
    def modify_sql_select(self, sql_command):


        sql_stmt = sql_command[4:]      # Remove the type of change needed
        change_id = sql_command[0:4]    #  <00> = test for all

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
                # Add formatting - to_char(file_time, 'YYYY-MM-DD HH24:MI:SS.MS')
                sql_stmt = "%sto_char(%s, 'YYYY-MM-DD HH24:MI:SS.MS')%s" % (sql_stmt[:index_start], sql_stmt[index_start + 17:index_end], sql_stmt[index_end+15:])
                end_pos = index_start - 15

        return sql_stmt

    # =======================================================================================================================
    # In Postgres, one connection for all threads
    # =======================================================================================================================
    def is_thread_connection(self):
        return False

    # =======================================================================================================================
    # When row are retrieved, need to call query_row_by_row()
    # =======================================================================================================================
    def is_row_by_row(self):
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
    # Reclaim database space
    # =======================================================================================================================
    def reclaim_space(self, status: process_status):
        return True

    # =======================================================================================================================
    #  Returns True if the engine API manage pool of connections
    # =======================================================================================================================
    def with_connection_pool(self):
        return False        # Although a connection POOL is used, it is considered as no pool to the driver (the implementation is hidden)

    # =======================================================================================================================
    # Return the database size in BYTES
    # =======================================================================================================================
    def get_dbms_size(self, status):

        sql_string = "SELECT pg_database_size('%s');" % self.dbms_name

        ret_val, data_string = self.select_as_json(status, "", sql_string, [])

        if not ret_val:
            size = "-1"
        else:
            # Value format: '[(8485831,)]'
            if len(data_string) > 5 and data_string[2:-3].isdecimal():
                size = data_string[2:-3]
            else:
                size = "-1"

        return size
    # ==================================================================
    # try creating database (if not exits) and connect to it
    # ==================================================================
    def connect_to_db(self, status, user, passwd, host, port, dbn, conditions):

        self.dbms_name = dbn

        self.ip_port = host + ':' + str(port)

        self.error_message = False

        if conditions:
            self.autocommit = interpreter.get_one_value_or_default(conditions, "autocommit", True)

        if not self.conn_pool:
            self.conn_pool = self.get_connection_pool(status, user, passwd, host, port, dbn)
            if not self.conn_pool:
                # Create the database for the first time
                ret_val = self.create_dbms(status, user, passwd, host, port, dbn)
                if ret_val == False:
                    return ret_val
                # try again after the database was created
                self.conn_pool = self.get_connection_pool(status, user, passwd, host, port, dbn)
                if not self.conn_pool:
                    status.add_message("PSQL Failed to connect to database '%s'" % dbn)
                    return False


        status.add_message("Connected to database '%s' using Postgres" % dbn)
        ret_val = True

        return ret_val

    # ==================================================================
    # Create first time database
    # ==================================================================
    def create_dbms(self, status, user, passwd, host, port, dbn ):

        postges_connect = self.get_single_connection(status, user, passwd, host, port, "postgres")
        if not postges_connect:
            status.add_error("Failed to connect to postgres to create database '%s'" % dbn)
            return False

        try:
            cursor = postges_connect.cursor()  # cursor on the postgres dbms
        except:
            status.add_error("Failed to init Postgres cursor for database '%s'" % dbn)
            self.close_single_connection(status, postges_connect)
            return False

        try:
            cursor.execute("CREATE DATABASE %s;" % dbn)
        except:
            status.add_error("Failed to create database - '%s'" % dbn)
            ret_val = False
        else:
            ret_val = True

        try:
            cursor.close()
        except:
            pass

        status.add_message("Database '%s' created on Postgres" % dbn)

        self.close_single_connection(status, postges_connect)

        return True


    # ==================================================================
    # Get a single connection from the pool
    # ==================================================================
    def get_conn_from_pool(self, status):
        try:
            connection = self.conn_pool.getconn()
            connection.autocommit = self.autocommit
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Postgres failed to get connection with database '{self.dbms_name}' from the pool [{errno}] [{value}]")
            connection = None

        return connection
    # ==================================================================
    # Return a single connection to the pool
    # ==================================================================
    def ret_conn_to_pool(self, status, connection):
        try:
            self.conn_pool.putconn(connection)
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Postgres failed to return connection with database '{self.dbms_name}' from the pool [{errno}] [{value}]")
            ret_val = False
        else:
            ret_val = True
        return ret_val

    # ==================================================================
    # Get Cursor - The cursor includes a tuple - connection + a cursor
    # ==================================================================
    def get_cursor(self, status):

        connection = self.get_conn_from_pool(status)
        if not connection:
            return None

        try:
            conn_cursor = connection.cursor()
        except:
            status.add_error("PSQL failed to get cursor with database %s" % self.dbms_name)
            return None

        return (connection, conn_cursor)  # Return the tupple

    # ==================================================================
    # Commit
    # ==================================================================
    def commit(self, status, db_cursor):

        try:
            db_cursor[0].commit()       # db_cursor[0] is the connection
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Failed to commit transaction with error {errno} : {value}")
            ret_val = process_status.Commit_failed
        else:
            ret_val = process_status.SUCCESS
        return ret_val

    # ==================================================================
    # Rollback
    # ==================================================================blockchain show operator
    def rollback(self, status, db_cursor):
        try:
            db_cursor[0].rollback()         # db_cursor[0] is the connection
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Failed to commit transaction with error {errno} : {value}")
            ret_val = process_status.Rollback_failed
        else:
            ret_val = process_status.SUCCESS
        return ret_val


    # ==================================================================
    # Get number of rows affected
    # ==================================================================
    def get_rows_affected(self, status, db_cursor):
        try:
            rows_count = db_cursor[1].rowcount
        except:
            status.add_error("Request for PostgreSQL rows count with invalid cursor")
            rows_count = 0
        return rows_count

    # ==================================================================
    # Get number of inserts in the file
    # ==================================================================
    def get_rows_inserted(self, status, db_cursor):
        try:
            rows_count = db_cursor[1].query.count(b'INSERT ')
        except:
            status.add_error("Request to count PostgreSQL rows inserted failed")
            rows_count = 0
        return rows_count

    # ==================================================================
    # Execute SQL as a stmts
    # ==================================================================
    def execute_sql_file(self, status: process_status, db_cursor, sql_file):

        ret_val = True
        file_path = os.path.expanduser(os.path.expandvars(sql_file))

        try:
            db_cursor[1].execute(open(file_path, 'r').read())
        except psycopg2.DataError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.InternalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.IntegrityError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.NotSupportedError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.ProgrammingError as e:
            error_msg = str(e)
            ret_val = False
        except (Exception, psycopg2.Error) as e:
            error_msg = str(e)
            ret_val = False
        except:
            error_msg = "Unknown error"
            ret_val = False


        if ret_val == False:
            error_msg = "Error executing SQL from file:\n%s\n%s" % (error_msg, sql_file)
            status.add_error(error_msg)
            status.keep_error(error_msg)

        rows_counter = self.get_rows_inserted(status, db_cursor)

        reply_list = [ret_val, rows_counter]

        return reply_list

    # ==================================================================
    # Execute SQL statement
    # ==================================================================
    def execute_sql_stmt(self, status: process_status, db_cursor, sql_stmt: str, ignore_error = False):

        ret_val = True

        try:
            db_cursor[1].execute(sql_stmt)
        except psycopg2.DataError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.InternalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.IntegrityError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.NotSupportedError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.ProgrammingError as e:
            error_msg = str(e)
            ret_val = False
        except (Exception, psycopg2.Error) as e:
            error_msg = str(e)
            ret_val = False
        except:
            error_msg = "Unknown error"
            ret_val = False
        else:
            error_msg = ""


        if utils_sql.is_trace_sql():
            # Enabled by: trace level = 1 sql command
            utils_sql.trace_sql("PSQL", sql_stmt, ret_val, ignore_error, error_msg)

        if ret_val == False:
            if ignore_error:
                ret_val = True
            else:
                error_msg = "Error executing SQL:\r\n%s\n%s" % (sql_stmt, error_msg)
                status.add_error(error_msg)
                status.keep_error(error_msg)


        return ret_val

    # ==================================================================
    # Fetch results for execute_sql_without_fetch
    # if fetch_size is 0, execute fetchall()
    # Return List
    # ==================================================================
    def fetch_list(self, status: process_status, db_cursor, fetch_size: int):

        ret_val = True
        try:
            if fetch_size:
                output = db_cursor[1].fetchmany(fetch_size)  # fetchmany is a list object
            else:
                output = db_cursor[1].fetchall()

        except psycopg2.DataError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.InternalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.IntegrityError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.NotSupportedError as e:
            error_msg = str(e)
            ret_val = False
        except psycopg2.ProgrammingError as e:
            error_msg = str(e)
            ret_val = False
        except (Exception, psycopg2.Error) as e:
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

        reply_list =  [ret_val, output]
        return reply_list

    # ==================================================================
    # Fetch results
    # if fetch_size is 0, execute fetchall()
    # Return String
    # ==================================================================
    def fetch_rows(self, status: process_status, db_cursor, output_prefix: str, fetch_size: int,
                   title_list, type_list):

        ret_val, output = self.fetch_list(status, db_cursor, fetch_size)

        output_len = len(output)  # the size of the list with the result set
        if ret_val and output_len:
            if output_prefix:
                string_data = utils_sql.format_db_rows(status, db_cursor[1], output_prefix, output, title_list, type_list)
            else:
                string_data = str(output)
        else:
            string_data = ""

        reply_list =  [ret_val, string_data]
        return reply_list

    # ==================================================================
    # Fetch all rows  - return as a json string
    # ==================================================================
    def select_as_json(self, status: process_status, output_prefix: str, sql_stmt: str, title_list: list):

        cursor = self.get_cursor(status)

        if cursor:
            if self.execute_sql_stmt(status, cursor, sql_stmt):
                ret_val, string_data = self.fetch_rows(status, cursor, output_prefix, 0, title_list, None)  # 0 is fetchall()
            else:
                ret_val = False
                string_data = ""
            self.close_cursor(status, cursor)
        else:
            ret_val = False
            string_data = ""

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

        reply_list = [ret_val, list_data]
        return reply_list

    # ==================================================================
    # Given a database name, and table name
    # generate a list of partitions
    # ==================================================================
    def get_table_partitions(self, status: process_status, logical_dbms: str, logical_table: str):

        if logical_table == '*':
            stmt = ("SELECT "
                    + "\n\ttable_name as par_name"
                    + "\nFROM information_schema.tables"
                    + "\nWHERE"
                    + "\n\ttable_catalog = '%s'"
                    + "\nAND"
                    + "\n\ttable_schema = 'public'"
                    + "\nAND"
                    + "\n\ttable_name LIKE 'par_%s'"
                    + "\n\tORDER BY table_name;")

            sql_string = stmt % (logical_dbms, "%")
        else:
            stmt = ("SELECT "
                    + "\n\ttable_name as par_name"
                    + "\nFROM information_schema.tables"
                    + "\nWHERE"
                    + "\n\ttable_catalog = '%s'"
                    + "\nAND"
                    + "\n\ttable_schema = 'public'"
                    + "\nAND"
                    + "\n\ttable_name LIKE 'par_%s_%s'"
                    + "\n\tORDER BY table_name;")

            sql_string = stmt % (logical_dbms, logical_table, "%")
        output_prefix = "Partitions." + logical_dbms + "." + logical_table
        ret_val, data_string = self.select_as_json(status, output_prefix, sql_string, [])

        reply_list = [ret_val, data_string]
        return reply_list

    # =======================================
    # Get the list of TSD tables
    # ======================================
    def get_tsd_tables_list(self, status):

        stmt = ("SELECT "
                + "\n\ttable_name as table_name"
                + "\nFROM information_schema.tables"
                + "\nWHERE"
                + "\n\ttable_catalog = 'almgm'"
                + "\nAND"
                + "\n\ttable_schema = 'public'"
                + "\nAND"
                + "\n\ttable_name LIKE 'tsd_%'"
                + "\n\tORDER BY table_name;")

        output_prefix = "Tables.almgm.tsd"
        ret_val, data_string = self.select_as_json(status, output_prefix, stmt, [])

        reply_list =  [ret_val, data_string]
        return reply_list

    # ==================================================================
    # Return True if a table exists
    # ==================================================================
    def is_table_exists(self, status: process_status, table_name: str):

        sql_string = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%s'" % table_name

        ret_val, data_string = self.select_as_json(status, "", sql_string, [])

        if ret_val:
            if len(data_string):
                ret_val = True
            else:
                ret_val = False
        return ret_val

    # =======================================
    # Given a database name, generate a list of tables in the database
    # SQL Example: SELECT table_name FROM information_schema.tables WHERE table_catalog = 'anylog_test' AND table_schema = 'public';
    # ======================================
    def get_database_tables(self, status: process_status, dbms_name: str):

        stmt = ("SELECT "
                + "\n\ttable_name"
                + "\nFROM information_schema.tables"
                + "\nWHERE"
                + "\n\ttable_catalog = '%s'"
                + "\nAND"
                + "\n\ttable_schema = 'public'"
                + "\n\tORDER BY table_name;")

        sql_string = stmt % dbms_name
        output_prefix = "Structure.Tables." + dbms_name

        ret_val, data_string = self.select_as_json(status, output_prefix, sql_string, [])

        reply_list = [ret_val, data_string]
        return reply_list

    # =======================================
    # Get table info
    # Return a list of columns and their data types
    # ======================================
    def get_column_info(self, status: process_status, dbms_name: str, table_name: str):
        stmt = ("SELECT "
                + "\n\tcolumn_name, "
                + "\ncase " \
                    # "\n\twhen data_type='character varying' THEN 'varchar('||character_maximum_length||')'"\
                  "\n\twhen data_type='character' THEN 'character('||character_maximum_length||')'" \
                    # "\n\twhen data_type='numeric' THEN 'numeric('||numeric_precision||','||numeric_scale||')'"\
                  "\n\telse data_type" \
                  "\nend as data_type, "
                + "\ncolumn_default "
                + "\nFROM"
                + "\n\tinformation_schema.columns"
                + "\nWHERE"
                + "\n\ttable_schema='public'"
                + "\nAND"
                + "\n\ttable_name='%s'"
                + "\n\tORDER BY ordinal_position;")

        sql_string = stmt % table_name

        ret_val, column_list = self.select_as_list(status, sql_string)

        #ret_val, data_string = self.execute_select_all(status, None, sql_string, [])

        column_info = []

        if ret_val:
            for index, entry in enumerate(column_list):
                # return a list: column_id, column_name, column_type, default_value, 0
                default_value = entry[2]
                if default_value and isinstance(default_value,str) and default_value.startswith("nextval("):
                    default_value = None    # Remove default value which is an instruction to postgres

                column_info.append((index, entry[0], entry[1], None, default_value))

        reply_list = [ret_val, column_info]
        return reply_list

    # =======================================
    # Get table info
    # Given a table name, generate a list of column corresponding to table
    # ======================================
    def get_table_info(self, status: process_status, dbms_name: str, table_name: str, info_type: str):

        if info_type == "columns":
            # For postgres from: https://www.postgresql.org/docs/9.1/infoschema-attributes.html
            # character_maximum_length - If data_type identifies a character or bit string type, the declared maximum length;
            # null for all other data types or if no maximum length was declared.
            stmt = ("SELECT "
                    + "\n\tcolumn_name, "
                    + "\ncase " \
                        # "\n\twhen data_type='character varying' THEN 'varchar('||character_maximum_length||')'"\
                      "\n\twhen data_type='character' THEN 'character('||character_maximum_length||')'" \
                        # "\n\twhen data_type='numeric' THEN 'numeric('||numeric_precision||','||numeric_scale||')'"\
                      "\n\telse data_type" \
                      "\nend as data_type"
                    + "\nFROM"
                    + "\n\tinformation_schema.columns"
                    + "\nWHERE"
                    + "\n\ttable_schema='public'"
                    + "\nAND"
                    + "\n\ttable_name='%s'"
                    + "\n\tORDER BY ordinal_position;")

        else:
            # only name
            stmt = ("SELECT "
                    + "\n\tcolumn_name"
                    + "\nFROM"
                    + "\n\tinformation_schema.columns"
                    + "\nWHERE"
                    + "\n\ttable_schema='public'"
                    + "\nAND"
                    + "\n\ttable_name='%s';")

        sql_string = stmt % table_name

        output_prefix = "Structure." + dbms_name + "." + table_name

        ret_val, data_string = self.select_as_json(status, output_prefix, sql_string, [])

        reply_list =  [ret_val, data_string]
        return reply_list

    # ==================================================================
    # Commands that needs to be executed when a temprary table is created
    # ==================================================================
    def commands_for_temp_table(self, table_name):

        # for performance, no need to log data in a temp table
        command = "alter table %s set unlogged;" % table_name
        return command

    # ==================================================================
    # Given a table name, gemerate a list of column corresponding to table
    # ==================================================================
    def get_columns_with_types(self, status: process_status, logical_dbms: str, table_name: str):
        """
        Get columns & data_type in table
        :args:
           table_name:str - table name
        :param:
           stmt:str - Query to get coluumns
        :return:
           list of column names & correpsonding data-type
        """
        output = []
        stmt = ("SELECT "
                + "\n\tcolumn_name, data_type"
                + "\nFROM"
                + "\n\tinformation_schema.columns"
                + "\nWHERE"
                + "\n\ttable_schema='public'"
                + "\nAND"
                + "\n\ttable_name='%s'"
                + "\n\tORDER BY ordinal_position;")

        sql_string = stmt % table_name
        output_prefix = "Structure." + logical_dbms + "." + table_name
        ret_val, data_string = self.select_as_json(status, output_prefix, sql_string, [])

        return data_string

    def get_columns_only(self, status: process_status, logical_dbms: str, table_name: str):
        """
        Return only column names
        :args:
           table_name:str - table name
        :return:
           list of table columns
        """
        results = self.get_columns_with_types(status, logical_dbms, table_name)
        data = utils_json.str_to_json(results)
        key = list(data)[0]
        columns = []
        for column in results:
            columns.append(column['column_name'])
        return columns

    def get_create_table(self, status: process_status, logical_dbms: str, table_name: str):
        """
        Return CREATE table
        :args:
           table_name:str - table name
        :return:
           create table
        """
        results = self.get_columns_with_types(status, logical_dbms, table_name)
        data = utils_json.str_to_json(results)
        key = list(data)[0]
        create_table = "CREATE TABLE IF NOT EXISTS  %s(" % table_name
        for column in results:
            create_table += "\n\t%s %s" % (column['column_name'], column['data_type'])
            if column == results[-1]:
                create_table += "\n);"
            else:
                create_table += ","

        return create_table

    # ==================================================================
    # connect to database
    # ThreadedConnectionPool - https://www.reddit.com/r/learnpython/comments/f0nieg/multithreadingmultiprocessing_postgres/
    # psycopg2.pool.SimpleConnectionPool(minConnection, maxConnection, *args, **kwargs)
    # getconn(key=None): To Get an available connection from the pool
    # putconn(connection, key=None, close=False): To Put away a connection. i.e., return a connection to the connection pool.
    # closeall(): Close all the connections handled by the pool
    # Details - https://pynative.com/psycopg2-python-postgresql-connection-pooling/
    # Example: threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 20, user="postgres",
    #                                                                     password="pass@#29",
    #                                                                     host="127.0.0.1",
    #                                                                     port="5432",
    #                                                                     database="postgres_db")
    # ==================================================================
    def get_connection_pool(self, status: process_status, usr: str, passwd, host: str, port: int, dbn: str):

        if is_supported:
            try:
                conn_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, user=usr,
                                                               password=passwd,
                                                               host=host,
                                                               port=port,
                                                               database=dbn)
            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(f"PostgreSQL Error - connection to db '{dbn}' failed - {value}")
                conn_pool = None
        else:
            status.add_error("PSQL database not installed")
            conn_pool = None

        return conn_pool

    # ==================================================================
    # A single connection is used to create the DBMS in the first time
    # ==================================================================
    def get_single_connection(self, status: process_status, usr: str, passwd, host: str, port: int, dbn: str):

        if is_supported:

            try:
                conn = psycopg2.connect(host=host, port=port, user=usr, password=passwd, dbname=dbn)
            except psycopg2.OperationalError as e:  # exit if fails
                if e != ('FATAL:  database "%s" does not exist' % dbn):
                    status.add_error("Connection to db '%s' failed - %s" % (dbn, str(e)))
                conn = None
            except:
                status.add_error("Connection to db '%s' failed " % dbn)
                conn = None
            else:
                conn.autocommit = True      # System DBMS
        else:
            status.add_error("PSQL database not installed")
            conn = None

        return conn

    # ==================================================================
    # Close a single onnection to database
    # ==================================================================
    def close_single_connection(self, status: process_status, db_connect):
        try:
            db_connect.close()
        except:
            status.add_error("PSQL failed to close connection")
            return False

        return True
    # ==================================================================
    # Close connection to database
    # ==================================================================
    def close_connection(self, status: process_status, db_connect):
        try:
            self.conn_pool.closeall()
        except:
            status.add_error("PSQL failed to close connection")
            return False

        return True

    # ==================================================================
    # Close Cursor
    # ==================================================================
    def close_cursor(self, status, db_cursor):
        try:
            db_cursor[1].close()
        except:
            status.add_error("PSQL failed to close connection in dbms %s" % self.dbms_name)

        ret_val = self.ret_conn_to_pool(status, db_cursor[0])
        return ret_val

    # =======================================
    # A DBMS that takes PostgreSQL calls returns True
    # A DBMS like PI that considers the generic calls returns False
    # ======================================
    def is_default_sql_command(self):
        return True

    # =======================================
    # Map rows to insert statements
    # ======================================
    def get_insert_rows(self, status: process_status, dbms_name: str, table_name: str, insert_size: int,
                        column_names: list, insert_rows: list):
        insert_statements = ""
        for row in insert_rows:
            # transform from an array of columns to a comma seperated string
            if not row:
                continue
            insert_stmt = "INSERT INTO %s VALUES (%s);\n" % (table_name, ','.join(row))
            insert_statements += insert_stmt
        return insert_statements
