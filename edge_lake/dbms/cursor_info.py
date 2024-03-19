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


# =======================================================================================================================
# Per each processing thread - Maintains DBMS Cursor per table
# =======================================================================================================================
class CursorInfo:

    def __init__(self):
        self.dbms_connect = None
        self.cursor = None  # this is the cursor provided by the specific physical database
        self.table_name = ""
        self.dbms_name = ""
        self.rows_count = 0  # number of rows returned with this cursor
        self.volume_read = 0  # volume od data read from the dbms with this cursor
        self.out_variable = ""  # A variable name to which the query data is assined
        self.assign_data = False  # A true value indicates that the data is to be assigned to a variable

    # -------------------------------------------------------------
    # If the database Cursor needs the data block received over the network
    # -------------------------------------------------------------
    def commit(self, status):
        self.dbms_connect.commit(status, self.cursor)

    # -------------------------------------------------------------
    # Keep the cursor object
    # -------------------------------------------------------------
    def set_cursor(self, db_cursor):
        self.cursor = db_cursor

    # -------------------------------------------------------------
    # Return the cursor object
    # -------------------------------------------------------------
    def get_cursor(self):
        return self.cursor

    # -------------------------------------------------------------
    # Keep the connection object and the dbms name
    # -------------------------------------------------------------
    def set_dbms(self, db_connect, dbms_name):
        self.dbms_connect = db_connect
        self.dbms_name = dbms_name

    # -------------------------------------------------------------
    # Return the connection object
    # -------------------------------------------------------------
    def get_db_connect(self):
        return self.dbms_connect

    # -------------------------------------------------------------
    # Return the Table name
    # -------------------------------------------------------------
    def get_table_name(self):
        return self.table_name

    # -------------------------------------------------------------
    # Set the Table name
    # -------------------------------------------------------------
    def set_table_name(self, table_name):
        self.table_name = table_name

    # -------------------------------------------------------------
    # Return the DBMS name
    # -------------------------------------------------------------
    def get_dbms_name(self):
        return self.dbms_name

    # -------------------------------------------------------------
    # Assign the data to a variable
    # -------------------------------------------------------------
    def set_assign_data(self, variable_name):
        self.assign_data = True
        self.out_variable = variable_name

    # -------------------------------------------------------------
    # Test if to assign the data to a variable
    # -------------------------------------------------------------
    def is_assign_data(self):
        return self.assign_data

    # -------------------------------------------------------------
    # Get the variable name to store the output data
    # -------------------------------------------------------------
    def get_out_var_name(self):
        return self.out_variable

    # -------------------------------------------------------------
    # Reset the object
    # -------------------------------------------------------------
    def reset_query(self):
        self.rows_count = 0
        self.volume_read = 0
        self.out_variable = 0
        self.assign_data = False
