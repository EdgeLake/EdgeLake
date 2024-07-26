"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


# generic_dir = os.path.expanduser(os.path.expandvars('$HOME/EdgeLake/source/generic'))
# sys.path.insert(0, generic_dir)

import time

import edge_lake.dbms.cursor_info as cursor_info
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.dbms.partitions as partitions
import edge_lake.dbms.unify_results as unify_results
import edge_lake.generic.params as params
from edge_lake.generic.process_log import add
from edge_lake.dbms.sqlite_dbms import get_dbms_connect_info
from edge_lake.generic.utils_data import get_string_hash
from edge_lake.dbms.dbms import connect_dbms, is_blobs_dbms
from edge_lake.generic.stats import operator_update_inserts


active_dbms = {}  # DBMS info as f(dbms name)
active_tables = {}  # Table info as f(dbms name+table)
active_views = {}  # View info as f(dbms name+view)


# =======================================
# Create new database
# ======================================
def create_dbms(db_name, dbms, db_type, connection_pool):
    '''
    db_name - the logical dbms name
    dbms - the DBMS object
    db_type - the physical database (Postgre, SQLite, Mongo ...)
    connection_pool - True - if the driver manage connections
    '''
    global active_dbms
    active_dbms[db_name] = {'connection': dbms, 'db type': db_type, 'pool' : connection_pool}
# =======================================
# Get DBMS connection
# ======================================
def get_connection(db_name: str):
    try:
        if active_dbms[db_name]["pool"]:
            connection = active_dbms[db_name]['connection'].get_connection(db_name)  # AnyLog connector manages a pool of connections
        else:
            connection = active_dbms[db_name]['connection']

    except:
        connection = None

    return connection

# =======================================
# Return True if DBMS with connection Pool
# Returns the same answer as active_dbms[db_name]['connection'].with_connection_pool()
# ======================================
def with_connection_pool(dbms_name):

    try:
        ret_val = active_dbms[dbms_name]["pool"]
    except:
        ret_val = False
    return ret_val
# =======================================
#The engine manage a free list - add connection to the free list pool
# ======================================
def free_db_connection(dbms_name, db_connect):  # Place the connection on the free list
    try:
        active_dbms[dbms_name]['connection'].free_connection(db_connect)
    except:
        pass

# =======================================
# Get the IP and port of the connected DBMS
# ======================================
def get_dbms_ip_port(status, db_name):
    db_connect = get_connection(db_name)
    if db_connect == None:
        status.add_keep_error("Database \'%s\' not connected" % db_name)
        return ":"
    return db_connect.get_ip_port()


# =======================================
# Update a list with the blobs databases
# =======================================
def get_blobs_dbms_list( dbms_list:list ):
    global active_dbms

    for dbms_name, dbms_info in active_dbms.items():
        db_type = dbms_info["db type"]
        if is_blobs_dbms(db_type):
            dbms_list.append(dbms_name)

# =======================================
# A DBMS that takes PostgreSQL calls returns True
# A DBMS like PI that considers the generic calls returns False
# ======================================
def is_default_sql_command(db_name):
    global active_dbms
    return active_dbms[db_name]['connection'].is_default_sql_command()


# =======================================
# Process a single SQL Statement (which is not select)
# If failed - place the sql stmt error in the log
# ======================================
def non_select_sql_stmt(status, dbms_name, sql_command):
    if not process_contained_sql_stmt(status, dbms_name, sql_command):
        status.add_error("Failed to process DDL with a SQL stmt: " + sql_command)
        ret_val = process_status.ERR_SQL_failure
    else:
        ret_val = process_status.SUCCESS
    return ret_val


# =======================================
# Insert a list of SQL statements to a database
# =======================================
def insert_rows(status, dbms_name, table_name, list_inserts):

    dbms_start_time = time.time()

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("Database \'%s\' not connected" % dbms_name)
        return False

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        status.add_keep_error("Database \'%s\' failed to provide a cursor" % dbms_name)
        return False

    ret_val = True
    for insert_stmt in list_inserts:
        if not insert_stmt:
            continue            # List entry can be empty
        ret_val = db_connect.execute_sql_stmt(status, db_cursor, insert_stmt)
        if not ret_val:
            break
    
    # commit or if error than rollback
    if ret_val:
        db_connect.commit(status, db_cursor)
        dbms_process_time = - time.time() - dbms_start_time
        operator_update_inserts(dbms_name, table_name, len(list_inserts), True, dbms_process_time)  # Update stat on inserts
    else:
        db_connect.rollback(status, db_cursor)

    db_connect.close_cursor(status, db_cursor)

    return ret_val
# =======================================
# Process a single SQL Statement (which is not select)
# Get dbms object -> get cusrosr -> exec. sql -> close cursor
# ======================================
def process_contained_sql_stmt(status, dbms_name, sql_command, ignore_error = False):
    '''
    status - the process status
    dbms_name - te logical sdatabase name
    sql_command - The command to execute
    ignore error - ignore error returned from the execution of SQL
    '''
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("Database \'%s\' not connected" % dbms_name)
        return False

    if not db_connect.is_sql_storage():
        status.add_keep_error("Database \'%s\' is not a sql database (use 'file remove' to drop a table from a blob database)" % dbms_name)
        return False


    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        status.add_keep_error("Database \'%s\' failed to provide a cursor" % dbms_name)
        return False

    command_str = sql_command

    if command_str[:10].lower() == "drop table":
        commands, sql_to_exec = db_connect.modify_sql_drop(command_str)   # make changes which are specific to the database
    elif command_str[:6].lower() == "create":
        commands, sql_to_exec = db_connect.modify_sql_create(command_str)  # make changes which are specific to the database
    else:
        commands = [len(command_str) - 1]  # an array for commands offsets
        sql_to_exec = command_str

    start_offset = 0
    need_commit = False
    sql_executed = False
    for end_offset in commands:
        sql_executed = True
        # some databases like SQLite need to see indexes in a separate call
        ret_val = db_connect.execute_sql_stmt(status, db_cursor, sql_to_exec[start_offset:end_offset + 1], ignore_error)
        if not ret_val:
            break
        if not need_commit:
            # need to validate only once
            if sql_to_exec[start_offset:start_offset + 7].lower() != "select ":
                need_commit = True

        start_offset = end_offset + 1

    if sql_executed:
        if need_commit:
            # commit or if error than rollback
            if ret_val:
                db_connect.commit(status, db_cursor)
            else:
                db_connect.rollback(status, db_cursor)
    else:
        status.add_error("The SQL command was not executed: '%s'" % sql_to_exec)
        ret_val = process_status.SQL_not_executed

    db_connect.close_cursor(status, db_cursor)

    return ret_val


# =====================================================================
# Get a command that will be issued when a table is a temporary table
# =====================================================================
def commands_for_temp_table(db_name, table_name):
    db_connect = get_connection(db_name)
    if db_connect == None:
        commands = ""
    else:
        commands = db_connect.commands_for_temp_table(table_name)
    return commands


# =======================================
# Process a SQL Statement from a file
# ======================================
def process_sql_from_file(status, dbms_name, table_name, file_path):

    dbms_start_time = time.time()
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        reply_list =  [False, 0]
        return reply_list

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        ret_val = False
        rows_counter = 0
    else:


        ret_val, rows_counter = db_connect.execute_sql_file(status, db_cursor, file_path)

        if ret_val:
            db_connect.commit(status, db_cursor)
        else:
            db_connect.rollback(status, db_cursor)

        db_connect.close_cursor(status, db_cursor)

        dbms_process_time = time.time() - dbms_start_time

        operator_update_inserts(dbms_name, table_name, rows_counter, False, dbms_process_time) # Update stat on inserts

    return [ret_val, rows_counter]
# =======================================
# Process a single delete Statement i.e. Insert / Delete
# ======================================
def process_contained_stmt(status, dbms_name, sql_command):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        reply_list = [False, 0]
        return reply_list

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        reply_list = [False, 0]
        return reply_list
    else:

        ret_val = db_connect.execute_sql_stmt(status, db_cursor, sql_command)

        if ret_val:
            db_connect.commit(status, db_cursor)
        else:
            db_connect.rollback(status, db_cursor)

        if ret_val:
            rows = db_connect.get_rows_affected(status, db_cursor)
        else:
            rows = 0

        db_connect.close_cursor(status, db_cursor)

    return [ret_val, rows]
# =======================================
# Process a single select Statement - Fetch all rows with this call
# Get dbms object -> get cusrosr -> exec. sql -> close cursor
# ======================================
def process_contained_select_stmt(status, dbms_name, sql_command, output_prefix, title_list):
    """

    :param status: AnyLog status handle
    :param dbms_name: Logical DBMS
    :param sql_command: Select stmt
    :param output_prefix: Prefix to the JSON output
    :param title_list: A list with names of columns retrieved (optional)
    :return:
    """

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        reply_list = [False, ""]
        return reply_list

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        reply_list = [False, ""]
        return reply_list
    else:

        ret_val = db_connect.execute_sql_stmt(status, db_cursor, sql_command)
        if ret_val:
            # select all rows
            get_next, str_data = db_connect.fetch_rows(status, db_cursor, output_prefix, 0, title_list, None)
        else:
            get_next = False
            str_data = ""

        db_connect.close_cursor(status, db_cursor)

    reply_list = [get_next, str_data]
    return reply_list


# =======================================
# process select stmt and return a list of rows
# ======================================
def select_rows_list(status, dbms_name, sql_command, list_size):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_error("Database \'%s\' not declared for process" % dbms_name)
        reply_list = [False, ""]
        return reply_list

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        reply_list = [False, ""]
        return reply_list
    else:

        if sql_command[0] == '<':
            # A query that needs to be modified by the dbms engine - examples are in events.py
            sql_stmt = db_connect.modify_sql_select(sql_command)       # Pre process the SQL according to the engine type
        else:
            sql_stmt = sql_command

        ret_val = db_connect.execute_sql_stmt(status, db_cursor, sql_stmt)
        if ret_val:
            # select all rows
            get_next, str_data = db_connect.fetch_list(status, db_cursor, list_size)
        else:
            get_next = False
            str_data = ""

        db_connect.close_cursor(status, db_cursor)

    reply_list = [get_next, str_data]
    return reply_list


# =======================================
# Process a SQL Statement in a sequence of statements
# Cursor is kept open
# dbms_cursor is the AnyLog Cursor
# db_cursor is the database cursor
# ======================================
def process_sql_stmt(status, dbms_cursor, sql_command):
    db_connect = dbms_cursor.get_db_connect()
    db_cursor = dbms_cursor.get_cursor()  # get the database cursor
    ret_val = db_connect.execute_sql_stmt(status, db_cursor, sql_command)

    # commit or if error than rollback
    if ret_val:
        db_connect.commit(status, db_cursor)
    else:
        db_connect.rollback(status, db_cursor)

    return ret_val


# =======================================================================================================================
# When row are retrieved, the engine can retrieve row by row or send all rows in a single request
# =======================================================================================================================
def is_row_by_row(dbms_name):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        return False

    return db_connect.is_row_by_row()


# =======================================
# WIth Cursors that do not contain metadata
# push the metadata
# ======================================
def update_cusrosr(status, dbms_cursor):
    db_connect = dbms_cursor.get_db_connect()
    db_cursor = dbms_cursor.get_cursor()  # get the database cursor
    if not db_connect.is_self_contained_curosr():
        # This DBMS Cusror is not self contained - it needs info from AnyLog to process data
        dbms_name = dbms_cursor.get_dbms_name()
        table_name = dbms_cursor.get_table_name()
        table_struct = get_table_structure(dbms_name, table_name)
        db_cursor.set_table_struct(table_name, table_struct)


# =======================================
# Retrieve Rows using the cursor
# dbms_cursor is the AnyLog Cursor
# db_cursor is the database cursor
# ======================================
def process_fetch_rows(status, dbms_cursor, output_prefix, fetch_size, title_list, type_list):
    db_connect = dbms_cursor.get_db_connect()
    db_cursor = dbms_cursor.get_cursor()  # get the database cursor

    get_next, str_data = db_connect.fetch_rows(status, db_cursor, output_prefix, fetch_size, title_list, type_list)
    reply_list = [get_next, str_data]
    return reply_list


# =======================================
# Set a dbms CURSOR on the cursor object
# dbms_cursor is the AnyLog Cursor
# db_cursor is the database cursor
# ======================================
def set_cursor(status: process_status, dbms_cursor: cursor_info, dbms_name: str):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        return False

    dbms_cursor.set_dbms(db_connect, dbms_name)

    db_cursor = db_connect.get_cursor(status)
    if db_cursor == None:
        ret_val = False
    else:
        dbms_cursor.set_cursor(db_cursor)
        ret_val = True

    return ret_val


# =======================================
# CLose Connection
# ======================================
def close_connection(status, dbms_name: str):
    global active_dbms

    if with_connection_pool(dbms_name):
        try:
            ret_val = active_dbms[dbms_name]['connection'].close_all(status)
        except:
            status.add_error("Close connection failed: Database '%s' is not connected" % dbms_name)
            ret_val = False
    else:
        db_connect = get_connection(dbms_name)
        if db_connect == None:
            status.add_error("Close connection failed: Database '%s' is not connected" % dbms_name)
            return False

        ret_val = db_connect.close_connection(status, db_connect)

    if ret_val:
        try:
            del active_dbms[dbms_name]      # Remove from AnyLog's database listing
        except:
            pass

    return ret_val


# =======================================
# CLose cursor
# ======================================
def close_cursor(status: process_status, dbms_cursor: cursor_info):
    db_connect = dbms_cursor.get_db_connect()
    db_cursor = dbms_cursor.get_cursor()
    db_connect.close_cursor(status, db_cursor)


# =======================================
# Call the database to map rows to insert statements
# ======================================
def get_insert_rows(status: process_status, dbms_name: str, table_name: str, insert_size: int, column_names: list,
                    insert_rows: list):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        data_string = ""
    else:
        data_string = db_connect.get_insert_rows(status, dbms_name, table_name, insert_size, column_names, insert_rows)

    return data_string


# =======================================
# Get table info
# Given a table name, return a list with columns info corresponding to table
# ======================================
def get_column_info(status, dbms_name: str, table_name: str):
    d_name = dbms_name.lower()
    t_name = table_name.lower()

    db_connect = get_connection(d_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        column_info = None
    else:
        ret_val, column_info = db_connect.get_column_info(status, d_name, t_name)

    return column_info

# =======================================
# Get table info
# Given a table name, generate a list of column corresponding to table
# ======================================
def get_table_info(status, dbms_name: str, table_name: str, info_type: str):
    d_name = dbms_name.lower()
    t_name = table_name.lower()
    db_connect = get_connection(d_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        data_string = ""
    else:
        ret_val, data_string = db_connect.get_table_info(status, d_name, t_name, info_type)
    return data_string


# =======================================
# Test if a database is connected
# ======================================
def is_dbms_connected(status, dbms_name: str):
    db_connect = get_connection(dbms_name.lower())
    if db_connect == None:
        ret_val = False
    else:
        ret_val = True

    return ret_val


# =======================================
# Get view info
# Given a view name, generate a list of column corresponding to table
# ======================================
def get_view_info(status, dbms_name: str, view_name: str, info_type: str):
    global active_views

    d_name = dbms_name.lower()
    v_name = view_name.lower()
    data_string = ""
    if d_name in active_views.keys():
        if v_name in active_views[d_name].keys():
            columns = active_views[d_name][v_name]
            columns_string = ""
            for entry in columns:
                # column names and types
                columns_string += "{\"column_name\":\"" + entry[0] + "\",\"data_type\":\"" + entry[
                    1] + "\",\"using\":\"" + entry[2] + "\"},"

            data_string = "{\"View." + d_name + "." + v_name + "\":[" + columns_string[:-1] + "]}"

    return data_string


# =======================================
# Get the list of partitions
# Info type can be - all, first or last
# ======================================
def get_table_partitions(status, dbms_name: str, table_name: str, info_type: str):
    d_name = dbms_name.lower()
    t_name = table_name.lower()

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        data_string = ""
    else:
        ret_val, data_string = db_connect.get_table_partitions(status, d_name, t_name)
        ret_val, data_string = partitions.filter_list(status, d_name, t_name, data_string, info_type)

    return data_string

# =======================================
# Get the list of partitions
# Info type can be - all, first or last
# ======================================
def get_tsd_table_list(status):

    db_connect = get_connection("almgm")
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % "almgm")
        data_string = ""
        ret_val = False
    else:
        ret_val, data_string = db_connect.get_tsd_tables_list(status)

    reply_list = [ret_val, data_string]
    return reply_list

# =======================================
# Get the list of partitions
# Info type can be - all, first or last
# ======================================
def get_table_partitions_list(status, dbms_name: str, table_name: str):
    d_name = dbms_name.lower()
    t_name = table_name.lower()

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        par_list = None
    else:
        ret_val, data_string = db_connect.get_table_partitions(status, d_name, t_name)
        ret_val, par_list = partitions.par_str_to_list(status, d_name, t_name, data_string, "par_name")

    return par_list


# =======================================
# Get the list of partitions with additional info on each partition
# ======================================
def get_partitions_info(status, dbms_name: str, table_name: str):
    d_name = dbms_name.lower()
    t_name = table_name.lower()

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        data_string = ""
    else:
        ret_val, data_string = db_connect.get_table_partitions(status, d_name, t_name)
        if ret_val and data_string:
            info_list = partitions.get_info_partitions(status, d_name, t_name, data_string)
            if not info_list:
                data_string = None
            else:
                data_string = utils_print.output_nested_lists(info_list, "", ["Partition Name", "Start Date", "End Date"], True)

    return data_string
# =======================================
# Get a list with the partition name in each partition
# ======================================
def get_partitions_list(status, dbms_name: str, table_name: str):

    par_list = None

    d_name = dbms_name.lower()
    t_name = table_name.lower()

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        ret_val = process_status.DBMS_NOT_COMNNECTED
    else:
        ret_code, data_string = db_connect.get_table_partitions(status, d_name, t_name)
        ret_val = process_status.Missing_par_info
        if ret_code and data_string:
            par_dict = utils_json.str_to_json(data_string)
            if par_dict:
                list_key = f"Partitions.{dbms_name}.{table_name}"
                if isinstance(par_dict, dict) and list_key in par_dict:
                    par_list = par_dict[list_key]
                    ret_val = process_status.SUCCESS

    return [ret_val, par_list]

# =======================================
# Get tables in a database
# Given a database name, retrieve the list of tables in the database
# ======================================
def get_database_tables(status, dbms_name: str):
    '''
    Return the list of tables in a string format
    '''

    d_name = dbms_name.lower()

    db_connect = get_connection(d_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        data_string = ""
    else:
        ret_val, data_string = db_connect.get_database_tables(status, d_name)

        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list


    return data_string
# =======================================
# Get tables in a database
# Given a database name, retrieve the list of tables in the database
# ======================================
def get_database_tables_list(status, dbms_name: str):
    '''
    Return the list of tables in a LIST format
    '''

    if dbms_name[:6] == "blobs_" and len(dbms_name) > 6:
        # get the list from the SQL database
        d_name = dbms_name[6:].lower()
        blobs_dbms = True
    else:
        d_name = dbms_name.lower()
        blobs_dbms = False

    info_string = get_database_tables(status, d_name)
    if info_string:
        info_json = utils_json.str_to_json(info_string)

        json_key = "Structure.Tables.%s" % d_name
        if info_json and json_key in info_json:
            tables_list = info_json[json_key]
            if blobs_dbms:
                # Remove the partitioned table names
                for index in range (len(tables_list) -1, -1, -1):
                    entry = tables_list[index]
                    if "table_name" in entry:
                        if entry["table_name"].startswith("par_"):
                            del tables_list[index]      # will not have data in the blob storage

        else:
            tables_list = None
    else:
        tables_list = None

    if tables_list:
        # Sort by name
        tables_list.sort(key=retrieve_table_name)
    return tables_list

# =======================================
# Get the table name from a dictionary
# ======================================
def retrieve_table_name(name_dict):
    if "table_name" in name_dict:
        key = name_dict["table_name"]
    else:
        key = ""
    return key

# =======================================
# Given a database name, gemerate a list of views in the database
# ======================================
def get_database_views(status, dbms_name: str):
    global active_views

    if dbms_name in active_views.keys():
        views_list = active_views[dbms_name]
        views_string = ""
        for view_name in views_list.keys():
            views_string += "{\"view_name\":\"" + view_name + "\"},"
        if views_string:
            data_string = "{\"Structure.Views.%s\":[" % dbms_name + views_string[:-1] + "]}"
        else:
            data_string = ""
    else:
        data_string = ""
    return data_string


# =======================================
# Validate that the databases are connected
# and the list of tables exists
# ======================================
def validate_tables(status, dbms_tables: list):
    ret_val = process_status.SUCCESS
    for dbms_dot_table in dbms_tables:
        # each entry is dbms_name.table_name
        entry = dbms_dot_table.strip()
        index = entry.find('.')
        if index <= 0 or index == (len(entry) - 1):
            status.add_error("Wrong dbms and table specification with the entry: %s" % entry)
            ret_val = process_status.ERR_process_failure
            break
        dbms_name = entry[:index]
        if not is_dbms(dbms_name):
            status.add_error("Operator is not connected to dbms: '%s'" % dbms_name)
            ret_val = process_status.DBMS_NOT_COMNNECTED
            break
        table_name = entry[index + 1:]
        if not is_table_exists(status, dbms_name, table_name):
            status.add_error("Operator failed to identify table: '%s.%s'" % (dbms_name, table_name))
            ret_val = process_status.Table_struct_not_available
            break
    return ret_val


# =======================================
# Test if a table exists (given dbms name and table name)
# ======================================
def is_table_exists(status, dbms_name: str, table_name: str):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_error("DBMS '%s' is not connected" % dbms_name)
        ret_val = False
    else:
        ret_val = db_connect.is_table_exists(status, table_name)

        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return ret_val
# =======================================
# Is Active Status
# ======================================
def is_active_stat(db_name: str):
    global active_dbms

    if not db_name in active_dbms.keys():
        return False
    return True


# =======================================
# Is database type
# ======================================
def test_db_type(db_name, db_type: str):
    global active_dbms
    try:
        ret_val = active_dbms[db_name]['db type'] == db_type
    except:
        ret_val = False
    return ret_val


# =======================================
# Get database type
# ======================================
def get_db_type(db_name):
    global active_dbms
    try:
        db_type = active_dbms[db_name]['db type']
    except:
        db_type = ""
    return db_type


# =======================================
# Is valid DBMS
# ======================================
def is_dbms(db_name: str):
    global active_dbms
    return db_name in active_dbms.keys()
# =======================================
# Get an array of dictionary elements
# ======================================
def get_dbms_array():
    global active_dbms
    return list(active_dbms)

# =======================================
# Get the number of databases
# ======================================
def count_dbms():
    global active_dbms
    return len(list(active_dbms.keys()))


# =======================================
# Get the list of databases
# ======================================
def get_dbms_list(status):
    global active_dbms

    title = ["Logical DBMS", "Database Type", "IP:Port", "Configuration", "Storage"]
    info_list = []
    for key in sorted(list(active_dbms)):
        dbms = active_dbms[key]['connection']

        info_list.append((key, active_dbms[key]['db type'],  dbms.get_ip_port(), dbms.get_config(status), dbms.get_storage_type()))

    reply = utils_print.output_nested_lists(info_list, "Active DBMS Connections", title, True)
    return reply
# =======================================
# Get the dictionary with databases info
# ======================================
def get_dbms_dict():
    global active_dbms

    reply_dict = {}
    for key in sorted(list(active_dbms)):
        dbms = active_dbms[key]['connection']
        db_type = active_dbms[key]['db type']
        ip_port = dbms.get_ip_port().ljust(30)[:30]
        storage_type = dbms.get_storage_type()
        reply_dict[key] = {
            "dbms_type" : db_type,
            "ip_port" : ip_port,
            "storage" : storage_type
        }

    return reply_dict
# =======================================
# Get the list of connected databases
# ======================================
def get_connected_databases():
    global active_dbms
    return list(active_dbms)
# =======================================
# Get active tables by DBMS name
# ======================================
def get_tables(db_name: str):
    global active_tables
    return active_tables[db_name]

# =======================================
# set table metadata in RAM
# ======================================
def set_table(db_name: str, table_name: str, table_struct: dict):
    global active_tables
    try:
        table_dctionary = active_tables[db_name]
    except:
        table_dctionary = {}
        active_tables[db_name] = table_dctionary

    active_tables[db_name][table_name] = table_struct


# =======================================================================================================================
# Test that the table definition exists
# =======================================================================================================================
def is_table_defs(dbms_name: str, table_name: str):
    global active_tables
    try:
        active_tables[dbms_name][table_name]["Structure." + dbms_name + '.' + table_name]
        ret_value = True
    except:
        ret_value = False
    return ret_value


# =======================================================================================================================
# Test that the view definition exists
# =======================================================================================================================
def is_view_defs(dbms_name: str, table_name: str):
    global active_views

    try:
        active_views[dbms_name][table_name]
        ret_value = True
    except:
        ret_value = False
    return ret_value


# =======================================================================================================================
# Maintain a virtual view of the data
# =======================================================================================================================
def set_view(status, db_name, create_view_stmt):
    global active_views
    ret_val, table_name, column_list = utils_sql.create_to_column_info(status, create_view_stmt)
    if not ret_val:

        try:
            view = active_views[db_name]
        except:
            view_dctionary = {}
            active_views[db_name] = view_dctionary

        active_views[db_name][table_name] = column_list

    return ret_val


# =======================================
# Test if a view defined (given dbms name and the view name)
# ======================================
def is_view_exists(status, dbms_name: str, view_name: str):
    global active_views
    if dbms_name in active_views.keys() and view_name in active_views[dbms_name].keys():
        ret_val = True
    else:
        ret_val = False

    return ret_val


# =======================================================================================================================
# If table metadata not in memory - load metadata from local database
# =======================================================================================================================
def load_table_info(status, dbms_name: str, table_name: str, return_no_data: bool):
    ret_val = process_status.SUCCESS
    if not is_table_defs(dbms_name, table_name):
        # Get metadata info
        info_str = get_table_info(status, dbms_name, table_name, "columns")
        if info_str == "":
            if return_no_data:
                # If the table is not defined, return no data:
                # AN operator may decloare that he supports a table, gets queries, but the table was not yet defined
                ret_val = process_status.Empty_data_set
            else:
                dbms_type = get_db_type(dbms_name)   # SQLIte /psql
                status.add_keep_error("Table '%s' in DBMS '%s' not defined (using '%s')" % (table_name, dbms_name, dbms_type))
                ret_val = process_status.No_metadata_info
        else:
            # Make Dictionary and place in RAM
            table_struct = utils_json.str_to_json(info_str)
            if isinstance(table_struct, dict):
                set_table(dbms_name, table_name, table_struct)
            else:
                status.add_keep_error(
                    "Failed to interpet metadata from dbms '%s' and table '%s'" % (dbms_name, table_name))
                ret_val = process_status.No_metadata_info
    return ret_val


# =======================================================================================================================
# Get the structure of a table as f(dbms_name and a table name)
# =======================================================================================================================
def get_table_structure(dbms_name: str, table_name: str):
    global active_tables
    try:
        struct_table = active_tables[dbms_name][table_name]["Structure." + dbms_name + '.' + table_name]
    except:
        struct_table = None

    return struct_table


# =======================================================================================================================
# Get the structure of a table as f(dbms_name and a table name) in a string format for a message to a different node
# =======================================================================================================================
def get_table_structure_as_str(dbms_name: str, table_name: str):
    global active_tables
    try:
        struct_table = active_tables[dbms_name][table_name]
        str_table = utils_json.to_string(struct_table)
    except:
        str_table = ""

    return str_table


# ==================================================================
# Get number of fields defining a view or a table
# ==================================================================
def get_counter_fields(dbms_name: str, table_name: str):
    fields_array = get_view_columns_list(dbms_name, table_name)
    if not fields_array:
        fields_array = get_table_structure(dbms_name, table_name)

    return len(fields_array)

# ==================================================================
# Make a create statement from the local database schema
# ==================================================================
def generate_create_stmt(status, dbms_name, table_name):

    column_info = get_column_info(status, dbms_name, table_name)
    if column_info and len(column_info):
        create_stmt = "CREATE TABLE IF NOT EXISTS %s (" % table_name
        index_stmt = ""
        for entry in column_info:
            column_name = entry[1]
            if column_name == "row_id":
                create_stmt += "row_id SERIAL PRIMARY KEY,"
            else:
                column_type = entry[2]
                if column_type.startswith("timestamp "):
                    create_stmt += " %s TIMESTAMP NOT NULL," % column_name
                    # Create an index on timestamp columns
                    index_stmt += " CREATE INDEX %s_%s_index ON %s(%s);" % (table_name, column_name, table_name, column_name)
                else:
                    column_type = utils_sql.get_al_data_types(column_type)

                    create_stmt += " %s %s," % (column_name, column_type)

        create_stmt = create_stmt[:-1] + ');' + index_stmt
    else:
        create_stmt = ""


    return create_stmt
# ==================================================================
# Based on the fields array, generate the fields for create stmt
# unify_data_type mapps to a unified supported data types
# ==================================================================
def get_create_fields(dbms_name: str, table_name: str, unify_data_types: dict):
    fields_str = ""
    fields_array = get_view_columns_list(dbms_name, table_name)
    if not fields_array:
        fields_array = get_table_structure(dbms_name, table_name)
    for column in fields_array:
        if fields_str != "":
            fields_str += ","  # not the first
        try:
            # column can be a list or a dictionary
            if isinstance(column, list) or isinstance(column, tuple):
                column_name = column[0]
                data_type = column[1]
            elif isinstance(column, dict):
                # Fields from table definition
                column_name = column["column_name"]
                data_type = column["data_type"]
            else:
                break

            if data_type in unify_data_types.keys():
                data_type = unify_data_types[data_type]

            fields_str += (column_name + " " + data_type)
        except:
            break

    return fields_str


# ==================================================================
# Based on the fields array, generate the list of fields names
# ==================================================================
def get_fields_names_types(projection_id, dbms_name: str, table_name: str, add_counter: bool):
    field_counter = projection_id
    fields_str = ""
    fields_array = get_view_columns_list(dbms_name, table_name)
    use_view = True

    data_type_array = []
    if not fields_array:
        use_view = False
        fields_array = get_table_structure(dbms_name, table_name)

    for column in fields_array:

        if fields_str != "":
            fields_str += ","  # not the first

        if add_counter:
            fields_str += "\"" + str(field_counter) + "\":\""

        try:
            # column can be a list or a dictionary
            if isinstance(column, list) or isinstance(column, tuple):
                if use_view:
                    fields_str += (column[0])
                    data_type_array.append(column[1])
                else:
                    # use table
                    fields_str += (column[1])
                    data_type_array.append(column[1])
            elif isinstance(column, dict):
                fields_str += (column["column_name"])
                data_type_array.append(column["data_type"])
            else:
                break
        except:
            break

        if add_counter:
            fields_str += "\""
            field_counter += 1

    reply_list = [fields_str, data_type_array, field_counter]
    return reply_list


# ==================================================================
# Return column id from a list.
# Every entry in the list is a tuple in the format:
# <JSON Attribute Name><Table Column Name><Data Type><Default Value>
# ==================================================================
def get_column_id(columns_list: list, column_name: str):
    for index, entry in enumerate(columns_list):
        if entry[1] == column_name:
            return index
    return -1  # not found


# ==================================================================
# Return column dictionary showing:
# column_id, and column_type as f (column_name)
# ==================================================================
def get_table_columns_dictionary(dbms_name: str, table_name: str):
    columns_list = get_table_columns_list(dbms_name, table_name)
    if columns_list:
        column_dict = {}
        for index, column in enumerate(columns_list):
            column_dict[column["column_name"]] = (index, column["data_type"])
    else:
        column_dict = None

    return column_dict


# ==================================================================
# Return column dictionary showing mapping:
# (view_id), (column_type), (table column name) as f (view column_name)
# ==================================================================
def get_view_columns_dictionary(dbms_name: str, table_name: str):
    columns_list = get_view_columns_list(dbms_name, table_name)
    if columns_list:
        column_dict = {}
        for index, column in enumerate(columns_list):
            # (view column_name)   (view_id)   (column_type)  (table column name)
            column_dict[column[0]] = (index, column[1], column[2])
    else:
        column_dict = None

    return column_dict


# ==================================================================
# Use the vew info to return a column name and type
# The local name is the name declared by the VIEW whereas column_name is with the SQL stmt
# Column info includes: <view column name> <data type> <using column name>
# ==================================================================
def get_view_column_info(dbms_name: str, view_name: str, column_name: str):
    global active_views
    column_info = ""
    index = 0  # returns the column id
    if dbms_name in active_views.keys():
        if view_name in active_views[dbms_name].keys():
            columns = active_views[dbms_name][view_name]
            for index, entry in enumerate(columns):
                if entry[0].lower() == column_name or entry[2].lower() == column_name:
                    column_info = entry

    reply_list = [index, column_info]
    return reply_list


# ==================================================================
# Return column type by column name
# ==================================================================
def get_column_type(dbms_name: str, table_name: str, column_name: str):
    index, view_info = get_view_column_info(dbms_name, table_name, column_name)  # get the view info on the column
    if view_info:
        column_type = view_info[1]
    else:
        index, column_type = get_table_column_number_and_type(dbms_name, table_name, column_name)

    if not column_type:
        add("Error",
            "The column '%s' is not declared with DBMS: '%s' and table: '%s'" % (column_name, dbms_name, table_name))

    return column_type


# ==================================================================
# Return column id and type by column name using table definitions
# ==================================================================
def get_table_column_number_and_type(dbms_name: str, table_name: str, column_name: str):
    column_type = ""
    index = 0
    table_struct = get_table_structure(dbms_name, table_name)
    if table_struct:
        for index, column in enumerate(table_struct):
            try:
                # column can be a list or a dictionary
                if isinstance(column, list):
                    if column[0] == column_name:
                        column_type = column[1]
                        break
                elif isinstance(column, dict):
                    if column["column_name"] == column_name:
                        column_type = column["data_type"]
                        break
                else:
                    column_type = None
                    break
            except:
                column_type = None
                break

    reply_list = [index, column_type]
    return reply_list


# ==================================================================
# Format DBMS error messages
# Using this class as the node displaying may not be connected to the dbms
# ==================================================================
def format_db_err_messages(err_msg: str, format_type: str):

    nessage_array = err_msg.replace('\r','').replace('\n',' ').replace("\"", "'")

    if format_type == "json":
        reply_data = ",{\"SQL Failure\": \"" + nessage_array + "\"}"
    else:
        reply_data = nessage_array

    return reply_data


# ==================================================================
# Drop DBMS
# ==================================================================
def drop_dbms(status, dbms_type, dbms_name, user, password, host, port):
    ret_val = process_status.Drop_DBMS_failed
    if is_dbms(dbms_name):
        status.add_error("DBMS is active - 'drop dbms' failed - call 'disconnect dbms %s' before the drop" % dbms_name)
    else:
        if dbms_type == 'psql':
            # Use the database 'postgres' to connect to psql
            if not is_dbms("postgres"):
                # Create the database
                postgres_dbms = connect_dbms(status, "postgres", "psql", user, password, host, port, False, "", None)
                if postgres_dbms:
                    create_dbms(postgres_dbms.get_dbms_name(), postgres_dbms, dbms_type, postgres_dbms.with_connection_pool())
            if not test_db_type("postgres", "psql"):
                status.add_error("'postgres' DBMS is not declared for PSQL")
            else:
                if process_contained_sql_stmt(status, "postgres", "drop database %s" % dbms_name):
                    ret_val = process_status.SUCCESS
        elif dbms_type == "sqlite":
            # need to delete the file on the OS
            dbms_name, connect_name, dbms_path = get_dbms_connect_info(dbms_name, False)
            if utils_io.delete_file(connect_name):
                ret_val = process_status.SUCCESS
            else:
                status.add_error("Failed to delete the SQLite DBMS file: %s" % connect_name)
        elif dbms_type == "mongo":

            db_connect = get_connection(dbms_name)
            if not db_connect:
                # No open connections on the database - connect
                new_dbms = connect_dbms(status, dbms_name, dbms_type, user, password, host, port, False, "", None)
                if new_dbms:
                    if new_dbms.drop_database(status, dbms_name):
                        ret_val = process_status.SUCCESS
            else:
                status.add_error("Failed to drop the MongoDB DBMS: %s" % dbms_name)
        else:
            ret_val = process_status.Wrong_dbms_type

    return ret_val


# ==================================================================
# create a local table to include the blockchain data
# The table structure: AutoIncr, HashValue (Unique Key), Company, Type , Date, Host, status, Policy
# host - The IP and Port of the server issued the call
# Example select: sql blockchain text "select * from ledger"
# ==================================================================
def blockchain_create_local_table(status):
    if not is_table_exists(status, "blockchain", "ledger"):

        sql_create = "CREATE TABLE ledger (" \
                     "sequence_id SERIAL PRIMARY KEY," \
                     "policy_type VARCHAR NOT NULL," \
                     "policy_id VARCHAR NOT NULL," \
                     "company_name VARCHAR," \
                     "date TIMESTAMP NOT NULL DEFAULT NOW()," \
                     "host VARCHAR NOT NULL," \
                     "status VARCHAR," \
                     "policy VARCHAR); " \
                     "\nCREATE UNIQUE INDEX hash_index ON ledger(policy_id);" \
                     "\nCREATE INDEX time_index ON ledger(date);"


        ret_value = process_contained_sql_stmt(status, "blockchain", sql_create)
    else:
        ret_value = True

    return ret_value

# ==================================================================
#   Add an entry to the tsd_info table (Time Series Data Info)
#   Determine the table based on the [TSD member] field
#   File name is structured as: [dbms name].[table name].[data source].[hash value].[instructions].[TSD member].[TSD ID].[TSD date].json
#   Return a ROW ID which identifies the insert and the time
# ==================================================================
def tsd_insert_entry(status, node_member_id, source_metadata):

    if source_metadata.tsd_member:
        # A file provided in the DISTRIBUTION process from a different member of the cluster

        file_time = source_metadata.tsd_date
        if not file_time:
            utc_date_time = utils_columns.get_current_utc_time("%Y-%m-%d %H:%M:%S")
        else:
            utc_date_time = utils_io.key_to_utc_timestamp(file_time)

        row_id = source_metadata.tsd_row_id
        table_name_prefix = source_metadata.tsd_member

        if not isinstance(table_name_prefix, int):
            status.add_error("Wrong TSD table name: tsd_%s" % str(table_name_prefix))
            ret_val = process_status.Wrong_tsd_id
            row_id = "0"
            file_time = ""
        else:

            sql_insert = "insert into tsd_%u (file_id, dbms_name, table_name, source, file_hash, instructions, file_time, rows, status1, status2) "\
                    " values ('%s','%s', '%s', '%s','%s', '%s', '%s', %s, '%s', '%s');" % (\
                    table_name_prefix, row_id, source_metadata.dbms_name, source_metadata.table_name, source_metadata.data_source,\
                    source_metadata.hash_value, source_metadata.instructions, utc_date_time, 0, source_metadata.tsd_status1, source_metadata.tsd_status2)

            ret_value = process_contained_sql_stmt(status, "almgm", sql_insert)
            if not ret_value:
                # if the table doesn't exist - create the table and try again
                if create_member_tsd(status, node_member_id, table_name_prefix):
                    # a new table was created, try the insert again
                    ret_value = process_contained_sql_stmt(status, "almgm", sql_insert)
                if not ret_value:
                    status.add_error("Failed to record info on new Time Series Data of table: '%s.%s' and member: '%s'" % (source_metadata.dbms_name, source_metadata.table_name, source_metadata.tsd_member))
                    row_id = "0"
                    file_time = ""
    else:
        # Insert to TSD_INFO representing data received from a data source (which us not a cluster member)
        row_id, file_time = insert_to_tsd_info(status, source_metadata.dbms_name, source_metadata.table_name, source_metadata.data_source, source_metadata.hash_value,\
                           source_metadata.instructions, source_metadata.tsd_status1, source_metadata.tsd_status2)

    reply_list =  [row_id, file_time]
    return reply_list
# ==================================================================
# Insert a new row to the TSD table that represents the local NODE (TSD_INFO)
# ==================================================================
def insert_to_tsd_info(status, dbms_name, table_name, data_source, hash_value, instructions, tsd_status1, tsd_status2):

    utc_date_time = utils_columns.get_current_utc_time("%Y-%m-%d %H:%M:%S.%f")
    file_time = utc_date_time[2:4] + utc_date_time[5:7] + utc_date_time[8:10] + utc_date_time[11:13] + utc_date_time[14:16] + utc_date_time[17:19]

    if hash_value:
        # From a JSON file (by the operator) -  the hash file is known
        unique_id = hash_value
    else:
        # From the streaming process, inserting individual rows, the hash is not yet known. Will be determined when the buffer is full.
        unique_id = dbms_name + '.' + table_name + '.' + file_time

    sql_insert = "insert into tsd_info (dbms_name, table_name, source, file_hash, instructions, file_time, rows, status1, status2) "\
         " values ('%s','%s', '%s', '%s','%s', '%s', 0,'%s','%s');" % (\
         dbms_name, table_name, data_source,\
         unique_id, instructions, utc_date_time, tsd_status1, tsd_status2)

    ret_value = process_contained_sql_stmt(status, "almgm", sql_insert)

    if ret_value:
        # The row was inserted, get a unique ID that represents the file
        row_id = tsd_row_id_select(status, unique_id)  # Query the TSD table to get the row_id
    else:
        status.add_error("Failed to record info on new Time Series Data of table: '%s.%s'" % (dbms_name, table_name))
        row_id = "0"
        file_time = ""

    reply_list = [row_id, file_time]
    return reply_list
# ==================================================================
# Update the status of an entry in tsd_info table (Time Series Data Info)
# ==================================================================
def tsd_update_entry(status, hash_value, status1, status2):
    if status1 and status2:
        sql_update = "update tsd_info set status1 = '%s', status2 = '%s' where file_hash = '%s';" % (
        status1, status2, hash_value)
    else:
        if status1:
            sql_update = "update tsd_info set status1 = '%s' where file_hash = '%s';" % (status1, hash_value)
        else:
            sql_update = "update tsd_info set status2 = '%s' where file_hash = '%s';" % (status2, hash_value)

    ret_value = process_contained_sql_stmt(status, "almgm", sql_update)

    return ret_value

# ==================================================================
# UpdateTSD INFO with the hash value of the file
# This is a process from the streaming data - when TSD is updated after the inserted rows
# ==================================================================
def tsd_update_streaming(status, hash_value, new_hash_value, row_count):

    sql_update = "update tsd_info set file_hash = '%s', rows = %u where file_hash = '%s';" % (new_hash_value, row_count, hash_value)

    ret_value = process_contained_sql_stmt(status, "almgm", sql_update)

    return ret_value

# ==================================================================
# Update the number of rows inserted to the DBMS
# ==================================================================
def tsd_update_rows(status, tsd_table_name, file_id, rows_count):

    sql_update = "update %s set rows = %u where file_id = %s;" % (tsd_table_name, rows_count, file_id)

    ret_value = process_contained_sql_stmt(status, "almgm", sql_update)

    return ret_value


# ==================================================================
# Create tsd_info table on the almgm database
# Time Series Data Info -> tsd_info
# Note : file_hash -  using the timestamp when updated by streaming data
# ==================================================================
def tsd_create_local_table(status):
    if not is_table_exists(status, "almgm", "tsd_info"):

        sql_create = "CREATE TABLE tsd_info (" \
                     "file_id SERIAL PRIMARY KEY NOT NULL," \
                     "dbms_name VARCHAR NOT NULL," \
                     "table_name VARCHAR NOT NULL," \
                     "source VARCHAR NOT NULL," \
                     "file_hash VARCHAR  NOT NULL," \
                     "instructions VARCHAR NOT NULL," \
                     "file_time TIMESTAMP NOT NULL," \
                     "rows INT," \
                     "status1 VARCHAR," \
                     "status2 VARCHAR); " \
                     "\nCREATE UNIQUE INDEX hash_index ON tsd_info(file_hash);" \
                     "\nCREATE INDEX time_index ON tsd_info(file_time);"

        ret_value = process_contained_sql_stmt(status, "almgm", sql_create)
    else:
        ret_value = True

    return ret_value
# =======================================================================================================================
# Create a TSD table for a different member in the cluster. The TSD table name is tsd_id - id is the member id in the cluster
# node_member_id - The id of this node in the cluster
# cluster_member_id - The member that is associated with the table created
# =======================================================================================================================
def create_member_tsd(status, node_member_id, cluster_member_id):

    table_name = "tsd_%s" % cluster_member_id

    if not is_table_exists(status, "almgm", table_name):

        # same struct as tsd_info but PK is not AutoIncr
        sql_create = "CREATE TABLE %s (" \
                     "file_id INT PRIMARY KEY NOT NULL," \
                     "dbms_name VARCHAR NOT NULL," \
                     "table_name VARCHAR NOT NULL," \
                     "source VARCHAR NOT NULL," \
                     "file_hash VARCHAR NOT NULL," \
                     "instructions VARCHAR NOT NULL," \
                     "file_time TIMESTAMP NOT NULL," \
                     "rows INT," \
                     "status1 VARCHAR," \
                     "status2 VARCHAR); " \
                     "\nCREATE UNIQUE INDEX hash_index_%s ON %s(file_hash);" \
                     "\nCREATE INDEX time_index_%s ON %s(file_time);" % (table_name, cluster_member_id, table_name, cluster_member_id, table_name)

        ret_value = process_contained_sql_stmt(status, "almgm", sql_create)
    else:
        # Return False because the insert failed, If the table exists the problem is not missing table
        ret_value = False

    return ret_value

# =======================================================================================================================
# Transforms a list with data retrieved from the TSD_info table to a file name
# file_id is a unique id in the TSD_info table
# =======================================================================================================================
def file_name_from_tsd_info(file_info, tsd_member_id, file_id):

    elements = len(file_info[0])

    if elements > 5:
        str_name = '.'.join(file_info[0][:5])        # Last field is date that needs to be formatted

        date_key = utils_io.utc_timestamp_to_key(file_info[0][-1])

        file_name ="%s.%s.%s.%s.json" % (str_name, tsd_member_id, file_id, date_key)
    else:
        file_name = ""

    return file_name
# =======================================================================================================================
# Delete a single row from TSD table
# =======================================================================================================================
def delete_tsd_row(status, table_name, row_id):

    sql_delete = f"delete from {table_name} where file_id = '{row_id}'"

    reply_val, rows = process_contained_stmt(status, "almgm", sql_delete)

    if reply_val:
        if rows == 1:
            ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.Wrong_dbms_key_in_delete
    else:
        ret_val = process_status.TSD_process_failed

    return ret_val

# ==================================================================
# Return the file name from the TSD table
# [dbms name].[table name].[data source].[hash value].[instructions].[timestamp]
# ==================================================================
def tsd_info_by_id(status, table_name, file_id) :

    sql_string = "<01>SELECT dbms_name, table_name, source, file_hash, instructions, <TIMESTAMP_START>file_time<TIMESTAMP_END> FROM tsd_info where file_id = '%s';" % file_id

    ret_val, data_list = select_rows_list(status, "almgm", sql_string, 0)

    reply_list = [ret_val, data_list]
    return reply_list
# ==================================================================
# Select the status of Time Series Data file from tsd_info
# ==================================================================
def tsd_info_select(status, table_name, sql_string):
    ret_code, data_list = select_rows_list(status, "almgm", sql_string, 0)
    if not ret_code:
        if not is_table_exists(status, "almgm", table_name):
            status.add_error("TSD query failed - wrong TSD table name: '%s'" % table_name)
            ret_val = process_status.ERR_table_name
        else:
            status.add_error("Failed to query the state of Time Series Data file using statement: '%s'" % sql_string)
            ret_val = process_status.Empty_result_set
    else:
        ret_val = process_status.SUCCESS

    reply_list = [ret_val, data_list]
    return reply_list
# ==================================================================
# Select the file id (which is the row id) from tsd_info
# ==================================================================
def tsd_row_id_select(status, hash_value: str):
    sql_string = "SELECT file_id FROM tsd_info where file_hash = '%s';" % hash_value

    ret_val, data_list = select_rows_list(status, "almgm", sql_string, 0)

    if ret_val and len(data_list):
        row_id = str(data_list[0][0])
    else:
        row_id = "0"  # indicate an error

    return row_id

# ==================================================================
# Count Rows
# ==================================================================
def get_rows_count(status,dbms_name:str, table_name:str):

    if len(dbms_name) > 6 and dbms_name[:6] == "blobs_":
        db_connect = get_connection(dbms_name)
        if db_connect == None:
            status.add_error("Database \'%s\' not declared for process" % dbms_name)
            rows_count = 0
        else:
            rows_count = db_connect.count_files(status, dbms_name, table_name, False)
            if with_connection_pool(dbms_name):
                free_db_connection(dbms_name, db_connect)  # Place the connection on the free list
    else:
        sql_string = "SELECT count(*) from %s;" % table_name

        ret_val, data_list = select_rows_list(status, dbms_name, sql_string, 0)

        if ret_val and len(data_list):
            rows_count = data_list[0][0]
        else:
            rows_count = 0

    return rows_count


# ==================================================================
# Drop a local table
# ==================================================================
def drop_table(status, dbms_name, table_name):
    if is_table_exists(status, dbms_name, table_name):

        sql_drop = "DROP TABLE %s;" % table_name

        ret_value = process_contained_sql_stmt(status, dbms_name, sql_drop)
    else:
        status.add_error("Drop table failed: The table '%s.%s' does not exists" % (dbms_name, table_name))
        ret_value = False

    return ret_value
# ==================================================================
# Add an entry to the blockchain ledger
# ==================================================================
def blockchain_insert_entry(status, host_name: str, policy: dict, ignore_error:bool):
    '''
    status - the thread status object
    host_name - The IP of the node that issued the policy
    policy - the policy data
    ignore_error - True/False to consider or ignore errors
    '''

    sql_insert = policy_to_insert(host_name, policy)

    ret_value = process_contained_sql_stmt(status, "blockchain", sql_insert, ignore_error)

    return ret_value

# ==================================================================
# Transform a JSON Policy to an insert statement
# ==================================================================
def policy_to_insert(host_name: str, policy:dict):
    '''
    host_name - The IP of the node that issued the policy
    policy - The policy to update
    '''
    key = next(iter(policy))

    json_object = policy[key]

    insert_columns = "insert into ledger (policy_type"
    insert_values = " values ('%s'" % key

    if "id" in json_object.keys():
        insert_columns += " ,policy_id"
        insert_values += " ,'%s'" % json_object["id"]

    if "company" in json_object.keys():
        insert_columns += ", company_name"
        insert_values += ", '%s'" % json_object["company"]

    if "date" in json_object.keys():
        insert_columns += ", date"
        insert_values += ", '%s'" % json_object["date"]

    insert_columns += ", host"
    insert_values += ", '%s'" % host_name

    if "status" in json_object.keys():
        insert_columns += ", status"
        insert_values += ", '%s'" % json_object["status"]

    json_data = utils_json.to_string(policy)

    policy_data = json_data.replace("'", "''")  # A single quotation in a database is replaced by 2

    insert_columns += ", policy)"
    insert_values += ", '%s')" % policy_data

    sql_insert = insert_columns + insert_values

    return sql_insert
# ==================================================================
# Delete an entry from the blockchain ledger
# ==================================================================
def blockchain_delete_entry(status, policy: dict):

    key = next(iter(policy))

    json_object = policy[key]

    if not "id" in json_object.keys():
        if "date" in json_object.keys():
            del json_object["date"]     # ID is calculated without the date
        json_string = str(policy)
        policy_id = get_string_hash('md5', json_string, None)
    else:
        policy_id = json_object["id"]

    return blockchain_delete_by_id(status, policy_id)

# ==================================================================
# Delete a policy using an ID
# ==================================================================
def blockchain_delete_by_id(status, policy_id):

    sql_delete = "delete from ledger where policy_id = '%s'" % policy_id

    reply_val, rows = process_contained_stmt(status, "blockchain", sql_delete)

    if reply_val:
        if rows == 1:
            ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.Wrong_dbms_key_in_delete
    else:
        ret_val = process_status.BLOCKCHAIN_operation_failed

    return ret_val

# ==================================================================
# Update a policy on the local database
# Host - the source machine issueing the call
# source ID - the ID of tyhe policy to change
# ==================================================================
def blockchain_update_entry(status, host, source_id,  policy):

    sql_string = "Update ledger set "  # Host may not be part of the policy

    policy_type = next(iter(policy))

    json_object = policy[policy_type]

    json_object["id"] = source_id       # Keep the source ID of the policy

    if "company" in json_object.keys():
        sql_string += "company_name = '%s', " % json_object["company"]

    if "date" in json_object.keys():
        sql_string += "date = '%s', " % json_object["date"]

    if "status" in json_object.keys():
        sql_string += "status = '%s', " % json_object["status"]

    sql_string += "host = '%s', " % host

    json_data = utils_json.to_string(policy)


    policy_data = json_data.replace("'", "''")  # A single quotation in a database is replaced by 2

    sql_string += "policy = '%s' where policy_id = '%s' and policy_type = '%s'" % (policy_data, source_id, policy_type)    # policy type validates that the same type of policy is updates

    ret_val = process_contained_sql_stmt(status, "blockchain", sql_string)

    return ret_val
# ==================================================================
# Select the blockchain data from a local database
# destinations:
# dbms retrieves insert statements that can be added to the local database
# log retrieved a copy of the log representing the blockchain
# ==================================================================
def blockchain_select(status, destination: str, filename: str, lock_key: str):

    sql_string = "SELECT host, policy FROM ledger order by date"     # Host may not be part of the policy

    ret_val, data_list = select_rows_list(status, "blockchain", sql_string, 0)

    if ret_val and data_list != "":
        updated_list = []

        for entry in data_list:
            # reset single quotation
            if isinstance(entry, tuple):
                # With SQLite
                if len(entry) == 2:
                    host = entry[0]
                    policy = entry[1].replace("\\'", "'")       # The policy entry

            if not host or not policy:
                status.add_error("Error in policy format: %s" % str(entry))
                ret_val = False
                break

            if filename == "stdout":
                json_entry = utils_json.str_to_json(policy)
                if json_entry:
                    utils_print.jput(json_entry, True)
                else:
                    status.add_error("Error in policy format: %s" % str(policy))
                    ret_val = False
                    break

            else:
                if destination == "sql":
                    # map to insert
                    json_entry = utils_json.str_to_json(policy)
                    if json_entry:
                        new_entry = policy_to_insert(host, json_entry)
                    else:
                        status.add_error("Error in policy format: %s" % str(policy))
                        ret_val = False
                        break
                else:
                    new_entry = policy

                updated_list.append(new_entry)

        if filename != "stdout":
            ret_val = utils_io.write_list_to_file(status, updated_list, filename, lock_key)
    else:
        ret_val = False

    return ret_val


# ==================================================================
# get the TABLE's columns list
# ==================================================================
def get_table_columns_list(dbms_name: str, table_name: str):
    global active_tables

    if dbms_name in active_tables.keys():
        if table_name in active_tables[dbms_name].keys():
            return active_tables[dbms_name][table_name]["Structure." + dbms_name + "." + table_name]
    return None


# ==================================================================
# get the VIEW's columns list
# ==================================================================
def get_view_columns_list(dbms_name: str, table_name: str):
    global active_views
    if dbms_name in active_views.keys():
        if table_name in active_views[dbms_name].keys():
            return active_views[dbms_name][table_name]
    return None


# ==================================================================
# Analyze the SQL stmt
# select_parsed is an object to contain the parsed sql - utils_sql.SelectParsed()
# ==================================================================
def select_parser(status, select_parsed, dbms_name, src_command, return_no_data, nodes_safe_ids):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS \"%s\" not connected" % dbms_name)
        reply_list =  [process_status.DBMS_NOT_COMNNECTED, "", src_command]
        return reply_list

    ret_val, is_select, new_string, offset_select = utils_sql.get_select_stmt(status, src_command)
    if not is_select or ret_val:
        # Not a select statement or ERROR in the select statement
        reply_list = [ret_val, "", src_command]
        return reply_list

    ret_val, sql_command = utils_sql.format_select_sql(status, new_string, offset_select, select_parsed)  # clean the sql stmt

    if ret_val:
        # Not a select statement or ERROR in the select statement
        reply_list = [ret_val, "", src_command]
        return reply_list

    engine_type = db_connect.get_engine_name()
    select_parsed.set_dbms_type(engine_type)

    if not select_parsed.parse_sql(status, dbms_name, sql_command):  # No need for - utils_sql.sql_to_standard_format(sql_satement.strip(" ;")) - after the call to format_select_sql
        reply_list = [process_status.Failed_to_parse_sql, "", src_command]
        return reply_list

    table_name = select_parsed.get_table_name()
    # Update remote and local database and table names

    if not table_name:
        status.add_keep_error("Failed to identify 'table name' in statement: \"%s\"" % src_command)
        reply_list =  [process_status.Failed_to_parse_sql, "", src_command]
        return reply_list

    select_parsed.set_target_names(dbms_name, table_name)

    if not db_connect.is_process_where():
        # This database engine does not support where condition, only returns rows within a time range
        if utils_sql.process_where_condition(status, select_parsed):
            status.add_keep_error("Failed to parse SQL 'where' conditions: \"%s\"" % src_command)
            reply_list = [process_status.Failed_to_parse_sql, "", src_command]
            return reply_list

    if not utils_sql.process_projection(status, select_parsed):
        status.add_keep_error("Failed to parse SQL projection list: \"%s\"" % src_command)
        reply_list =  [process_status.Failed_to_parse_sql, "", src_command]
        return reply_list

    if nodes_safe_ids:
        # If multiple nodes assigned to the same cluster - updated the where condition to data on the 2 nodes.
        select_parsed.update_cluster_status(nodes_safe_ids)     # This method relates to the HA - considering only data on all nodes

    if is_view_exists(status, dbms_name, table_name):
        with_view = True
    else:
        with_view = False
        ret_val = load_table_info(status, dbms_name, table_name, return_no_data)  # if not a view - load local table def to memory
        if ret_val:
            reply_list = [ret_val, "", src_command]
            return reply_list

    select_parsed.set_view(with_view)  # A user defined view

    is_suport_join = db_connect.is_suport_join()

    if unify_results.make_sql_stmt(status, select_parsed, is_suport_join):
        ret_val = process_status.SUCCESS
        sql_stmt = select_parsed.remote_query
    else:
        ret_val = process_status.Failed_to_parse_sql
        sql_stmt = src_command

    reply_list = [ret_val, table_name, sql_stmt]
    return reply_list

# ==================================================================
# Return the size of the dbms in bytes
# If the database does not exists - return -1
# ==================================================================
def get_dbms_size(status, dbms_name):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS \"%s\" not connected" % dbms_name)
        return "-1"

    reclaim_dbms_space(status, dbms_name)

    reply = db_connect.get_dbms_size(status)  # Implemented by each interface

    return reply


# ==================================================================
# Reclain database space if tables were dropped
# ==================================================================
def reclaim_dbms_space(status, dbms_name):
    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS \"%s\" not connected" % dbms_name)
        return False

    ret_val = db_connect.reclaim_space(status)  # Implemented by each interface

    return ret_val

# ==================================================================
# Update the local dbms with the policies of the provided file
# ==================================================================
def update_local_blockchain_dbms(status, policies, host, print_message):
    if not is_table_exists(status, "blockchain", "ledger"):
        status.add_keep_error("Table \'ledger\' in 'blockchain' dbms not available")
        return process_status.BLOCKCHAIN_operation_failed

    ret_val = process_status.SUCCESS
    updated_policies = 0
    for index, entry in enumerate(policies):
        if entry:
            json_obj = utils_json.str_to_json(entry)
            if not json_obj and print_message:
                message = "Blockchain update dbms - Policy is not represented as a JSON structure - entry number %u: '%s'" % (
                    index + 1, entry)
                utils_print.output(message, True)
                continue  # Skip this entry

            if blockchain_insert_entry(status, host, json_obj, True):  # ignore dulicate keys error
                updated_policies += 1

    if print_message:
        utils_print.output("\r\nPolicies added to local DBMS: %u from %u" % (updated_policies, len(policies)), True)
    return ret_val

# =======================================
# Insert Blob Data to a database
# ======================================
def store_file(status:process_status,  db_name: str, table_name: str, file_path: str, file_name: str, blob_hash_value: str, archive_date:str, ignore_duplicate:bool, trace:int):
    '''
            status - AnyLog status object
            db_name:str - logical database name
            table_name:str - the logical table name - supports file list by table name
            file_path:str - path file is located
            file_name:sr - file containing data
            blob_hash_value - unique file name
            archive_date - yy-mm-dd to allow search of files by date
            ignore_duplicate - if True, duplicate files do not return an error
    '''

    dbms_name = get_blobs_dbms_name(db_name)

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        ret_val = process_status.ERR_dbms_not_opened
    else:
        ret_val = db_connect.store_file(status, dbms_name, table_name, file_path, file_name, blob_hash_value, archive_date, ignore_duplicate, trace)

        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return ret_val
# =======================================
# Remove Blob Data from a database
# ======================================
def remove_file(status: process_status, db_name: str, table_name:str, id_str:str, hash_value, archive_date:str):
    '''
    status - AnyLog Status object
    dbms_name - the database assigned to the file
    id_str - the file ID
    table_name - if id_str is not provided
    archive_date - if id_str is not provided
    '''

    dbms_name = get_blobs_dbms_name(db_name)    # Can add the blobs_ prefix

    db_connect = get_connection(dbms_name)

    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        ret_val = process_status.ERR_dbms_not_opened
    else:

        if id_str or hash_value:
            ret_val = db_connect.remove_file(status, dbms_name, table_name, id_str, hash_value)
        else:
            ret_val = db_connect.remove_multiple_files(status, dbms_name, table_name, archive_date)


        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return ret_val

# =======================================
# Retrieve (one or more) Blob Data from a database to a file
# ======================================
def retrieve_file(status: process_status, db_name: str, db_filter:dict, limit, destination_name:str):
    '''
    status - AnyLog Status object
    dbms_name - the database assigned to the file
    id_str - the file ID
    destination_name - the file for the output
    '''

    dbms_name = get_blobs_dbms_name(db_name)

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        ret_val = process_status.ERR_dbms_not_opened
    else:

        dest_type = utils_io.determine_name_type(status, destination_name) # Returns 'file' or 'dir'

        if dest_type == "dir" and destination_name[-1] != params.get_path_separator():
            destination_name += params.get_path_separator()

        ret_val = db_connect.retrieve_files(status, dbms_name, db_filter, limit, dest_type, destination_name )
        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return ret_val



# =======================================================================================================================
#  Add "blobs_" to database name if needed
# =======================================================================================================================
def get_blobs_dbms_name(db_name):
    db_type = get_db_type(db_name)
    if not is_blobs_dbms(db_type):
        # Add blobs_  prefix if missing
        if not db_name.startswith("blobs_"):
            dbms_name = f"blobs_{db_name}"
        else:
            dbms_name = db_name
    else:
        dbms_name = db_name
    return dbms_name


# ==================================================================
# Count files
# ==================================================================
def get_files_count(status, db_name, table_name):

    ret_val = process_status.SUCCESS

    dbms_name = get_blobs_dbms_name(db_name)

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        counter = - 1
        per_table_count = None
        ret_val = process_status.ERR_dbms_not_opened
    else:
        counter, per_table_count = db_connect.count_files_per_table(status, dbms_name, table_name)

        if with_connection_pool(dbms_name):
            free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return [ret_val, counter, per_table_count]

# =======================================================================================================================
#  Get files list
# =======================================================================================================================
def get_files_list(status:process_status, db_name: str, table_name: str, id_str:str, hash_val:str, archive_date:str, limit:int):

    dbms_name = get_blobs_dbms_name(db_name)

    db_connect = get_connection(dbms_name)
    if db_connect == None:
        status.add_keep_error("DBMS '%s' not connected" % dbms_name)
        files_list = None
        ret_val = process_status.ERR_dbms_not_opened
    else:
        files_list = db_connect.get_file_list(status, dbms_name, table_name, id_str, hash_val, archive_date, limit)
        if not files_list:
            ret_val = process_status.Failed_to_list_files
        else:
            ret_val = process_status.SUCCESS

    if with_connection_pool(dbms_name):
        free_db_connection(dbms_name, db_connect)  # Place the connection on the free list

    return [ret_val, files_list]
