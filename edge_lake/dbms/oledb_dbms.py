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

import anylog_node.dbms.db_info as db_info
import anylog_node.tcpip.http_client as http_client
import anylog_node.cmd.member_cmd as member_cmd
import anylog_node.generic.process_status as process_status
import anylog_node.generic.utils_sql as utils_sql
import anylog_node.generic.utils_print as utils_print


class OLEDB:
    def __init__(self, engine_string, engine_name):

        self.connection_string = engine_string
        self.conn = None
        self.dbms_name = ""
        self.ip_port = ""
        self.engine_name = engine_name  # for example oledb.pi

    # =======================================================================================================================
    #  get engine name
    # =======================================================================================================================
    def get_engine_name(self):
        return self.engine_name

    # =======================================================================================================================
    #  get dbms name
    # =======================================================================================================================
    def get_dbms_name(self):
        return self.dbms_name

    # =======================================================================================================================
    #  Return True if the Cusror is self contained - it does not need any info from AnyLog
    # =======================================================================================================================
    def is_self_contained_curosr(self):
        return False  # The cursor implementation needs info

    # =======================================
    # A DBMS that takes PostgreSQL calls returns True
    # A DBMS like PI that considers the generic calls returns False
    # ======================================
    def is_default_sql_command(self):
        return False

    # =======================================================================================================================
    #  get IP and Port of the DBMS
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip_port

    # =======================================================================================================================
    #  Return the storage type
    # =======================================================================================================================
    def get_storage_type(self):
        return "Persistent"

    # =======================================================================================================================
    # When row are retrieved, there is no need to call query_row_by_row()
    # =======================================================================================================================
    def is_row_by_row(self):
        return True

    # =======================================================================================================================
    # Flag if join info is supported
    # =======================================================================================================================
    def is_suport_join(self):
        return True

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
        return False


    # =======================================================================================================================
    # make changes that are specific to the database
    # =======================================================================================================================
    def modify_sql_create(self, sql_command):

        commands_array = [len(sql_command) - 1]  # No changes are needed

        reply_list = [commands_array, sql_command]  # return all offset to commamds
        return reply_list

    # ==================================================================
    # Create a database
    # Default location is for the database is used, unless a path is specified
    # Example: connect dbms oledb anylog@http://localhost:demo 8080 test
    # connect dbms oledb anylog@http://localhost:demo 8080 pge_pi_central
    # ==================================================================
    def connect_to_db(self, status, user, passwd, host, port, dbn, conditions):

        self.ip_port = host + ":" + str(port)

        self.dbms_name = dbn

        # connect to the OLEDB Driver using REST

        rest_headers = {
            "type": "connect",
            "details": "open",
            "connect_str": self.connection_string,
            "dbms_name": dbn
        }

        reply_code, json_data = http_client.get_jdata(status, self.ip_port, rest_headers, None, 10)

        if not reply_code:
            if not json_data:
                status.add_keep_error("OLEDB connect: Failed to connect with the source database '%s' using: '%s'" % (
                dbn, self.connection_string))
                ret_val = False
            else:
                ret_val = True
        else:
            status.add_keep_error("OLEDB connect: Failed to sync with driver using: %s" % self.ip_port)
            ret_val = False

        return ret_val

    # ==================================================================
    # Close connection to database
    # ==================================================================
    def close_connection(self, status: process_status, db_connect):

        rest_headers = {
            "type": "connect",
            "details": "close",
            "dbms_name": self.dbms_name
        }
        reply_code, json_data = http_client.get_jdata(status, self.ip_port, rest_headers, None, 10)

        return True

    # =======================================================================================================================
    #  SETUP Calls - These are issued when the database is created
    # =======================================================================================================================
    def exec_setup_stmt(self, status):
        return True

    # =======================================================================================================================
    # Return info on the table - info is taken from the blockchain data
    # =======================================================================================================================
    def get_table_info(self, status, dbms_name, table_name, info_type):

        oledb_cursor = self.get_cursor(status)

        error_code = member_cmd.get_blockchain_table_info(status, oledb_cursor, dbms_name, table_name, info_type)
        if not error_code:
            # The table definitions were found
            string_data = db_info.get_table_structure_as_str(dbms_name, table_name)
            ret_val = True
        else:
            string_data = ""
            ret_val = False

        reply_list = [ret_val, string_data]
        return reply_list

    # =======================================
    # Get database info
    # Given a dbms name, get the list of tables
    # ======================================
    def get_database_tables(self, status: process_status, dbms_name: str):

        title_list = []
        output_prefix = "Tables." + dbms_name
        oledb_cursor = self.get_cursor(status)
        string_data = member_cmd.load_table_list_from_blockchain(status, oledb_cursor, output_prefix, title_list,
                                                                 dbms_name)

        reply_list = [True, string_data]
        return reply_list

    # =======================================================================================================================
    # Return a cursor
    # =======================================================================================================================
    def get_cursor(self, status):

        cursor = OLEDB_CURSOR(self.ip_port, self.dbms_name)
        return cursor

    # =======================================================================================================================
    # Close a cursor
    # =======================================================================================================================
    def close_cursor(self, status, db_cursor):
        pass

    # ==================================================================
    # Execute SQL statement
    # ==================================================================
    def execute_sql_stmt(self, status: process_status, db_cursor, sql_stmt: str):

        # - place SQL in Cursor

        ret_val = db_cursor.execute_sql_stmt(status, sql_stmt)

        return ret_val

    # ==================================================================
    # STEP 2 - Fetch rows
    # if fetch_size is 0, execute fetchall()
    # Return String
    # ==================================================================
    def fetch_rows(self, status, db_cursor, output_prefix, fetch_size, title_list):

        ret_code, output = db_cursor.fetch_rows(status, fetch_size)

        if not ret_code and output:
            ret_val = True
            string_data = utils_sql.format_db_rows(status, db_cursor, output_prefix, output, title_list, None)
        else:
            ret_val = False
            string_data = ""

        reply_list = [ret_val, string_data]

        return reply_list

# =======================================================================================================================
# OLEDB Cursor
# =======================================================================================================================
class OLEDB_CURSOR:
    description = [[""] * 2 for i in range(2)]

    def __init__(self, host_port, dbms_name):
        self.host_port = host_port
        self.sql_dbms_name = dbms_name  # the dbms name of the sql table
        self.sql = ""
        self.with_data = False  # If all the data set is maintained in the cusror
        self.data_index = 0  # If cusror holds all the data, data_index is a pointer on the next row
        self.data_set = None  # A ptr to the data set
        self.data_set_size = 0  # Updated with a new retrieved data set
        self.table_name = ""
        self.columns = ""
        self.query_rest_headers = {
            "type": "query",
            "details": "",
            "dbms_name": dbms_name
        }

    # ==================================================================
    # Return the SQL statement
    # ==================================================================
    def get_sql(self):
        return self.sql

    # ==================================================================
    # Keep table name and list of columns
    # ==================================================================
    def set_table_struct(self, table_name, table_struct):
        self.table_name = table_name
        self.columns = table_struct

    # ==================================================================
    # Execute the SQL statement against the OLEDB driver
    # Save the result as a list
    # Example our server: "select tag , time , value  from [piarchive]..[picomp]  where tag = \'sinusoid1\' and time between \'*-1h\' and \'*\'"
    # Example Transpara server: "SELECT TOP 100 eh.Name Element, ea.Name Attribute, a.Time, a.Value FROM [AMI6].[Asset].[ElementHierarchy] eh INNER JOIN [AMI6].[Asset].[ElementAttribute] ea ON ea.ElementID = eh.ElementID INNER JOIN [AMI6].[Data].[Archive] a ON a.ElementAttributeID = ea.ID WHERE eh.Path = N'\' AND ea.Name = 'Relative Humidity' AND a.Time BETWEEN N'*-1y' AND N'*' "
    # ==================================================================
    def execute_sql_stmt(self, status: process_status, sql_stmt: str):

        # sql_stmt= "SELECT TOP 100 eh.Name Element, ea.Name Attribute, a.Time, a.Value FROM [AMI6].[Asset].[ElementHierarchy] eh INNER JOIN [AMI6].[Asset].[ElementAttribute] ea ON ea.ElementID = eh.ElementID INNER JOIN [AMI6].[Data].[Archive] a ON a.ElementAttributeID = ea.ID WHERE eh.Path = N'\\' AND ea.Name = 'Relative Humidity' AND a.Time BETWEEN N'*-1y' AND N'*' "

        self.sql = sql_stmt
        self.with_data = False

        self.query_rest_headers["details"] = sql_stmt

        ret_val, returned_data = http_client.get_jdata(status, self.host_port, self.query_rest_headers, None, 600)

        if ret_val:
            ret_code = False
            self.data_set = None
            self.data_set_size = 0
        else:
            ret_code = True
            if not returned_data:
                # empty data set
                self.data_set = None
                self.data_set_size = 0
            else:
                if isinstance(returned_data, list):
                    self.data_set = returned_data  # projection list with columns list retun a list whereas every entry is a dictionary
                else:
                    self.data_set = [returned_data]  # projection list with functions return a dictionary
                self.data_set_size = len(self.data_set)

        self.data_index = 0  # A pointer to the row returned
        return ret_code

    # ==================================================================
    # Get a row from the data source
    # ==================================================================
    def fetch_rows(self, status, fetch_size):

        ret_val = process_status.SUCCESS

        if not self.data_set or self.data_index >= self.data_set_size:
            # No data or all data set was returned to the caller
            self.data_set = None  # Free the allocated memory for the data set
            data_rows = None
        else:
            rows = self.data_set[self.data_index:self.data_index + fetch_size]
            data_rows = self.unify_rows(rows)
            self.data_index += fetch_size

        reply_list =  [ret_val, data_rows]
        return reply_list

    # ==================================================================
    # Unify Rows - change the rows structure from source to AnyLog
    # Target is a list, each entry in the list is a single row
    # A single row is a list of column values
    # ==================================================================
    def unify_rows(self, source_rows):

        target_rows = []
        for row in source_rows:
            column_values = []
            target_rows.append(column_values)

            for value in row.items():
                column_values.append(value[1])
        return target_rows
