
from abc import ABC, abstractmethod

import edge_lake.generic.process_status as process_status

declared_tables_ = {}   # A dictionary with a list of tables that were created - this is used for performance optimization

class sql_storage(ABC):

    def __init__(self):
        self.dbms_name = ""
        self.engine_name = ""
    # =======================================================================================================================
    #  Return The engine name
    # =======================================================================================================================
    def get_engine_name(self):
        return self.engine_name

    # =======================================================================================================================
    #  Return True for SQL Storage
    # =======================================================================================================================
    def is_sql_storage(self):
        return True

    # =======================================================================================================================
    #  Return the logical database name
    # =======================================================================================================================
    def get_dbms_name(self):
        return self.dbms_name

    def retrieve_files(self, *args):
        return process_status.No_dbms_engine_support        # Used in blobs dbms

    def remove_multiple_files(self, *args):
        return process_status.No_dbms_engine_support        # Used in blobs dbms

    def remove_file(self, *args):
        return process_status.No_dbms_engine_support        # Used in blobs dbms

    def get_file_list(self, *args):
        return None                                         # Used in blobs dbms

    def count_files(self, *args):
        return -1                                           # Used in blobs dbms

    def store_file(self, *args):
        return False

    def is_stat_support(self):
        return False                                        # Can estimated number of rows be supported

    def estimate_rows(status, table_name, where_cond):
        return 0                                            # Estimate rows in a table (if supported by the database)