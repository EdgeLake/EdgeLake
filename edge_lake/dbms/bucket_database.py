
from abc import ABC, abstractmethod

import edge_lake.generic.process_status as process_status

declared_buckets_ = {}   # A dictionary with a list of buckets that were created - this is used for performance optimization

# Abstract bucket storage method
class bucket_storage(ABC):

    def __init__(self):
        self.dbms_name = ""
        self.engine_name = ""
        self.access_key = ""
        self.secret_key = ""
        self.region = ""
        self.ip_port = ""
        self.network_id = ""
        self.bucket_name = ""
        self.storage_type = "Bucket Persistent"
    # =======================================================================================================================
    #  Return The engine name
    # =======================================================================================================================
    def get_engine_name(self):
        return self.engine_name

    # =======================================================================================================================
    #  Return the storage type
    # =======================================================================================================================
    def get_storage_type(self):
        return self.storage_type

    # =======================================================================================================================
    #  Return True for SQL Storage
    # =======================================================================================================================
    def is_sql_storage(self):
        return False

    # =======================================================================================================================
    #  Return the logical database name
    # =======================================================================================================================
    def get_dbms_name(self):
        return self.dbms_name

    # =======================================================================================================================
    #  Return access_key
    # =======================================================================================================================
    def get_access_key(self):
        return self.access_key

    # =======================================================================================================================
    #  Return secret
    # =======================================================================================================================
    def get_secret_key(self):
        return self.secret_key

    # =======================================================================================================================
    #  Return region
    # =======================================================================================================================
    def get_region(self):
        return self.region

    # =======================================================================================================================
    #  Return endpoint
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip_port

    # =======================================================================================================================
    #  Return network id
    # =======================================================================================================================
    def get_network_id(self):
        return self.network_id

    # =======================================================================================================================
    #  Return bucket name
    # =======================================================================================================================
    def get_bucket_name(self):
        return self.bucket_name

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

    def estimate_rows(self, status, table_name, where_cond):
        return 0                                            # Estimate rows in a table (if supported by the database)

    def configure(self, key, value):
        '''
        Change default behaviour of the DBMS
        '''
        if hasattr(self, key):
            setattr(self, key, value)
            ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.ERR_attr_name
        return ret_val
