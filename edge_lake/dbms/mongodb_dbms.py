"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import threading


try:
    import gridfs
    import pymongo
    import_libs_failed = ""
except:
    mongo_installed = False
    import_libs_failed = "gridfs/pymongo"    # List the libraries for the error message
else:
    mongo_installed = True

import sys
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print


# ---------------------------------------------------------------
# AnyLog MongoDB Class
#
# Documentation - https://pymongo.readthedocs.io/en/stable/api/gridfs/index.html
# https://pymongo.readthedocs.io/en/stable/index.html
# ---------------------------------------------------------------
class MONGODB:
    def __init__(self):
        self.ip_port = None
        self.ip = None
        self.port = 0

        self.user = ""
        self.passwd = ""

        self.connect_counter = 0    # Total number of connections created
        self.free_list = []     # Free List of connections

        self.dbms_name = ""
        self.engine_name = "mongo"

        self.connection_mutex = threading.Lock()  # Global lock to add/remove connection

    def get_engine_name(self):
        return self.engine_name

    def get_dbms_name(self):
        return self.dbms_name

    def get_cursor(self, status):
        return None     # Not supported

    def get_database_tables(sekf, status, d_name):
        return [process_status.SUCCESS, None]     # Not supported

    # =======================================================================================================================
    #  get IP and Port of the DBMS
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip_port

    # =======================================================================================================================
    #  Return the storage type
    # =======================================================================================================================
    def get_storage_type(self):
        return "Blobs Persistent"

    # ---------------------------------------------------------------
    # issue calls that configure the database - these are specific calls for each dbms
    # ---------------------------------------------------------------
    def exec_setup_stmt(self,status):
        return True

    # =======================================================================================================================
    #  Returns True if the engine API manage pool of connections
    # =======================================================================================================================
    def with_connection_pool(self):
        return True

    # =======================================================================================================================
    # True is returned if the database supports SQL
    # =======================================================================================================================
    def is_process_where(self):
        return False

    # =======================================
    # Get table info
    # Given a table name, generate a list of column corresponding to table
    # ======================================
    def get_table_info(self, status: process_status, dbms_name: str, table_name: str, info_type: str):
        return [process_status.SQL_not_supported, ""]     # Not supported


    # =======================================
    # Close all connections
    # ======================================
    def close_all(self, status):

        self.connection_mutex.acquire()  # Take a mutex to avoid race condition
        if len(self.free_list) == self.connect_counter:
            # ALL connections are on the free list
            while ( len(self.free_list) ):
                mongo_instance = self.free_list.pop()
                mongo_instance.close_connection(status)
            ret_val = True
        else:
            status.add_error(f"MongoDB: Failed to close connections as DBMS {self.dbms_name} is active")
            ret_val = False

        self.connection_mutex.release()

        return ret_val

    # ---------------------------------------------------------------
    # Get the Mongo Connection
    # ---------------------------------------------------------------
    def connect_to_db(self, status, user, password, host, port, dbn, conditions):

        """
         Connection to MongoDB
         :potential params:
             https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
         :args:
             host:str - MongoDB IP address
             port:int - Mongo port
             conditions - can deliver specific connections params
         """

        if not mongo_installed:
            status.add_error("MongoDB Libraries (%s) not installed" % import_libs_failed)
            ret_val = False
        else:
            self.dbms_name = dbn
            self.ip = host
            if user:
                self.user = user
            if password:
                self.passwd = password
            try:
                self.port = int(port)
            except:
                status.add_error(f"MongoDB: Port value is not an int: {port}")
                ret_val = False
            else:
                ret_val = True
                self.ip_port = host + ':' + str(port)

        if ret_val:
            # Test the connection (as wrong ip/port does not return the error - need to call get_connection())
            connection = self.get_connection(status)
            if connection:
                self.free_connection(connection)
            else:
                ret_val = False

        return ret_val

    # ---------------------------------------------------------------
    # Get a single connection from the pool - or create a new connection
    # ---------------------------------------------------------------
    def get_connection(self, status):

        self.connection_mutex.acquire()     # Take a mutex to avoid race condition
        if len(self.free_list):
            mongo_connect = self.free_list.pop()    # get connection from the free list
        elif self.connect_counter > 20:
            status.add_error(f"MongoDB: Too many connections open: {self.connect_counter} conections for dbms '{self.dbms_name}'")
            mongo_connect = None            # Too many connections
        else:
            mongo_connect = None
            try:
                # Client Options - https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
                # https://kb.objectrocket.com/mongo-db/python-mongoclient-examples-1050
                # https://stackoverflow.com/questions/57443291/pymongo-errors-operationfailure-command-insert-requires-authentication 
                if self.user != "" and self.passwd != "":
                    connection = pymongo.MongoClient(host=self.ip, port=self.port, username=self.user, 
                                                     password=self.passwd, serverSelectionTimeoutMS=3000)
                else:
                    connection = pymongo.MongoClient(host=self.ip, port=self.port, serverSelectionTimeoutMS = 3000)
            except:
                errno, value = sys.exc_info()[:2]
                status.add_error("MongoDB: Failed to establish connection to MongoDB with error %s : %s" % (str(errno), str(value)))
            else:

                try:
                    connection.server_info()  # force connection
                except:
                    mongo_connect = None
                else:
                    try:
                        cur_large = gridfs.GridFS(connection[self.dbms_name])
                    except Exception as error:
                        err_msg = f'MongoDB: Failed to create a connection to store files in {self.dbms_name} (Error: {error})'
                        status.add_error(err_msg)
                        mongo_connect = None
                    else:
                        # Create new connection
                        mongo_connect = MONGO_INSTANCE(self.dbms_name, connection, cur_large)
                        self.connect_counter += 1

        self.connection_mutex.release()

        return mongo_connect

    # ---------------------------------------------------------------
    # Free the connection - place connection on the free list
    # ---------------------------------------------------------------
    def free_connection(self, db_connect):
        self.connection_mutex.acquire()  # Take a mutex to avoid race condition
        self.free_list.append(db_connect)
        self.connection_mutex.release()

    # ---------------------------------------------------------------
    # drop a logical database
    # This MONGODB object is isolated as it was done after disconnect dbms
    # Therefore no mutex is needed
    # ---------------------------------------------------------------
    def drop_database(self, status, dbms_name):

        ret_val = False

        try:
            connection = pymongo.MongoClient(host=self.ip, port=self.port)
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(
                "MongoDB: Failed to establish connection to MongoDB with error %s : %s" % (str(errno), str(value)))
        else:

            try:
                connection.drop_database(dbms_name)
            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(f"MongoDB: Failed to drop database '{dbms_name}': with error: [{errno}] [{value}]")

            else:
                ret_val = True

                try:
                    connection.close()
                except:
                    errno, value = sys.exc_info()[:2]
                    status.add_error(f"MongoDB: Failed to close connection after drop database '{dbms_name}': with error: [{errno}] [{value}]")

                else:
                    ret_val = True

        return ret_val


# --------------------------------------------------------------------------------------------
# A Thread safe connection that acts on the data
# --------------------------------------------------------------------------------------------
class MONGO_INSTANCE:

    def __init__(self, dbms_name, connection, cur_large):
        self.dbms_name = dbms_name
        self.conn = connection
        self.cur_large = cur_large

    # =======================================================================================================================
    #  Return False for SQL Storage
    # =======================================================================================================================
    def is_sql_storage(self):
        return False

    # ---------------------------------------------------------------
    # Returned but not supported
    # ---------------------------------------------------------------
    def get_database_tables(self, status: process_status, dbms_name: str):
        return [process_status.SUCCESS, ""]

    # ---------------------------------------------------------------
    # Get the list of databases
    # ---------------------------------------------------------------
    def get_dbms_list(self, status):

        try:
            database_names = self.conn.list_database_names()
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(
                "MongoDB: Failed to retrieve database name list with error %s : %s" % (str(errno), str(value)))
            database_names = None
        return database_names

    # ---------------------------------------------------------------
    # Connect Logical database
    # ---------------------------------------------------------------
    def connect_logical_database(self, status, db_name):
        """
        Declare logical database for both data and files
        :args:
            db_name:str - logical database name
        """
        global mongo_installed
        global import_libs_failed

        if not mongo_installed:
            status.add_error("MongoDB Libraries (%s) not installed" % import_libs_failed)
            ret_val = False
        else:
            ret_val = True
            if db_name not in self.cur:
                try:
                    self.cur[db_name] = self.conn[db_name]
                except Exception as error:
                    err_msg = f'Failed to create a connection to logical database {db_name} (Error: {error})'
                    status.add_error(err_msg)
                    ret_val = False
                else:
                    if db_name not in self.cur_large:
                        try:
                            self.cur_large[db_name] = gridfs.GridFS(self.cur[db_name])
                        except Exception as error:
                            err_msg = f'Failed to create a connection to store files in {db_name} (Error: {error})'
                            status.add_error(err_msg)
                            ret_val = False

        return ret_val


    # =======================================================================================================================
    #  Update Mongo with a file
    # =======================================================================================================================
    def store_file(self, status, dbms_name:str, table_name: str, file_path: str, file_name: str, blob_hash_value:str, archive_date:str, ignore_duplicate:bool, trace_level:int):
        """
        Store file in MongoDB using gridfs.grid_file.GridOut format
        :args:
            status - AnyLog status object
            db_name:str - logical database name
            table_name:str - the logical table name - supports file list by table name
            file_path:str - path file is located
            file_name:sr - file containing data
            blob_hash_value - unique file name
            archive_date - yy-mm-dd to allow search of files by date
            ignore_duplicate - True value means: if the file exists, the file is not added, but error is not returned
        :params:
            status:bool
            full_path:str - file path
        :return:
            status
        """
        ret_val = process_status.SUCCESS

        if dbms_name.startswith("blobs_") and len(dbms_name) > 6:
            sql_dbms = dbms_name[6:]
        else:
            sql_dbms = dbms_name


        full_path = os.path.expanduser(os.path.expandvars(os.path.join(file_path, f"{sql_dbms}.{table_name}.{file_name}")))
        # if file DNE print error message & return None as to not continue
        if not os.path.isfile(full_path):
            message = f'MongoDB (store file): Failed to locate image file: {file_name} - unable to continue'
            ret_val = process_status.Wrong_path
        else:

            per_table_hash = f"{table_name}.{blob_hash_value}"      # Allowing the same file in multiple tables
            per_table_name = f"{table_name}.{file_name}"  # Allowing the same file in multiple tables
            try:
                with open(full_path, 'rb') as f:
                    try:
                        if not self.is_duplicate(status, dbms_name, per_table_name):
                            file_data = f.read()
                            content = self.cur_large.put(file_data, filename=per_table_name, _id = per_table_hash, table = table_name, archive_date = archive_date)
                        else:
                            if ignore_duplicate == False:
                                # Duplicates do ot return an error - allowing multiple images to point to the same file
                                message = f'MongoDB (store file): Duplicate file name {per_table_name} in DBMS {dbms_name}'
                                ret_val = process_status.Already_exists # We have a unique index on file_name
                    except pymongo.errors.DuplicateKeyError:
                        message = f'MongoDB (store file): Duplicate Hash Value {per_table_hash} in DBMS {dbms_name}'
                        ret_val = process_status.Already_exists
                    except:
                        errno, value = sys.exc_info()[:2]
                        try:
                            if value.__class__.__name__ == "FileExists":
                                message = f'MongoDB: Duplicate Hash Value {per_table_hash} in DBMS {dbms_name}'
                                ret_val = process_status.Already_exists
                        except:
                            message = f'MongoDB (store file): Error updating database with file: {per_table_name} in DBMS: {dbms_name} with Error: {value}'
                            ret_val = process_status.DBMS_error
            except:
                errno, value = sys.exc_info()[:2]
                message = f'MongoDB (store file): Failed to open a file: {full_path} in DBMS: {dbms_name} with Error: {value}'
                ret_val = process_status.File_open_failed

        if ret_val:
            status.add_error(message)
            if trace_level:
                utils_print.output(message, True)


        return ret_val
    # =======================================================================================================================
    #  test duplicate file in the database
    # =======================================================================================================================
    def is_duplicate(self, status, dbms_name, per_table_name):
        try:
            if self.cur_large.exists(filename=per_table_name):
                ret_val = True
            else:
                ret_val = False
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"MongoDB: Failed to test if duplicate file using '{dbms_name}' and file: '{per_table_name}' with error: [{errno}] [{value}]")
            ret_val = True  # return as duplicate to avoid the write
        return ret_val

    # =======================================================================================================================
    #  file retrieve to a destination file
    # =======================================================================================================================
    def retrieve_files(self, status, dbms_name:str, db_filter:dict, limit:int, dest_type:str, destination_name:str ):
        '''
        status - AnyLog object status
        dbms_name
        table_name
        filter - the search criteria
        limit - max docs to retrieve - default is 1
        dest_type - file name ("file"), or, directory name ("dir")
        destination_name - the path for the output file (in case of a single file can include a name)
        '''

        ret_val = process_status.SUCCESS
        counter = 0

        if "_id" in db_filter or "filename" in db_filter:
            one_file = True     # These are unique keys - only one file retrieved
        else:
            one_file = False     # multiple files retrieved


        if dest_type == "file" and not one_file:
            status.add_error("MongoDB: Retrieve multiple files requires a folder as a destination - a file name was provided")
            ret_val = process_status.Err_dir_name
        elif dest_type == "file":
            dest_file = destination_name
        else:
            dest_file = None
            dest_folder = destination_name

        if not ret_val:

            try:
                fs = self.cur_large
                for grid_out in fs.find(db_filter, no_cursor_timeout=False):
                    if dest_file:
                        output_file = dest_file
                    else:
                        output_file = f"{dest_folder}{dbms_name}.{grid_out.name}"
                    data = grid_out.read()
                    if not utils_io.write_data_block(output_file, True, data):
                        status.add_keep_error("Retrieve File Error: '%s' Failed to write into file" % destination_name)
                        ret_val = process_status.ERR_write_stream
                        break

                    counter = counter + 1
                    if limit and counter >= limit:
                        break;

            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(f"MongoDB: Failed to retrieve documents from DBMS '{dbms_name}' and search by: '{db_filter}', with error {errno} : {value}" )
                ret_val = process_status.DBMS_error
            else:
                if counter == 0:
                    ret_val = process_status.No_file_in_storage


        return  ret_val

    # =======================================================================================================================
    #  Delete multiple files from storage
    # =======================================================================================================================
    def remove_multiple_files(self, status, dbms_name: str, table_name: str, archive_date: str):


        ret_val = process_status.SUCCESS
        filter = {}  # all tables in he database

        if table_name:
            filter["table"] = table_name

        if archive_date:
            filter["archive_date"] = archive_date  # In the format of YYMMDD

        counter = 0
        fs = self.cur_large

        try:
            for grid_out in fs.find(filter, no_cursor_timeout=False):
                try:
                    fs.delete(grid_out._id)
                except:
                    errno, value = sys.exc_info()[:2]
                    status.add_error(
                        f"MongoDB: Failed to remove documents list from DBMS '{dbms_name}', with error: {str(errno)} : {str(value)}")
                    ret_val = process_status.ERR_process_failure
                    break
                else:
                    counter += 1
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"MongoDB: Failed to retrieve documents list from DBMS '{dbms_name}', with error: {str(errno)} : {str(value)}")
            ret_val = process_status.ERR_process_failure
        else:
            if not counter:
                ret_val = process_status.No_file_in_storage

        return ret_val

    # =======================================================================================================================
    #  Delete a file from storage
    # content = self.cur_large.put(file_data, filename=per_table_name, _id = per_table_hash, table = table_name, archive_date = archive_date)
    # =======================================================================================================================
    def remove_file(self,  status, dbms_name, table_name, id_str, hash_value):

        ret_val = process_status.SUCCESS

        if hash_value:
            # Search by hash value
            id_filter = {"_id" : table_name + '.' + hash_value}
        else:
            # Search by name
            id_filter = {"filename": table_name + '.' + id_str}


        try:
            content = self.conn[dbms_name].fs.files.find_one(filter=id_filter)
        except Exception as error:
            status.add_error(f'MongoDB: Failed to locate content for file with id {id_str} (Error: {error})')
            ret_val = process_status.ERR_process_failure
        else:
            if content:
                try:
                    self.cur_large.delete(content['_id'])
                except Exception as error:
                    status.add_error(f'MongoDB: Failed to remove file with id {id_str} (Error: {error})')
                    ret_val = process_status.File_read_failed
            else:
                ret_val = process_status.No_file_in_storage

        return ret_val

    # =======================================================================================================================
    #  Get files list
    #  get files where dbms = blobs_edgex and table = image
    # =======================================================================================================================
    def get_file_list(self, status, dbms_name: str, table_name: str, id_str:str, hash_val:str, archive_date:str, limit:int):

        counter = 0
        total_size = 0
        files_name_list = []
        filter = {}  # all tables in he database

        if table_name:

            filter["table"] = table_name
            if id_str:
                filter["filename"] = table_name + '.' + id_str

            if hash_val:
                filter["_id"] = table_name + '.' + hash_val

        if archive_date:
            filter["archive_date"] = archive_date   # In the format of YYMMDD


        if dbms_name.startswith("blobs_") and len(dbms_name) > 6:
            print_name = dbms_name[6:]
        else:
            print_name = dbms_name

        try:
            fs = self.cur_large
            for grid_out in fs.find(filter, no_cursor_timeout=False):
                file_length = grid_out.length
                file_size = format(file_length,',').rjust(20)
                total_size += file_length
                file_name = f"{print_name}.{grid_out.name}"
                files_name_list.append(file_size + "   " + file_name)
                if limit:
                    counter = counter + 1
                    if counter >= limit:
                        break
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error("MongoDB: Failed to retrieve documents list from DBMS '%s' and table: '%s', with error %s : %s" % (
                                                            dbms_name, table_name, str(errno), str(value)))
            files_name_list = None

        if files_name_list:
            files_name_list.append(format(total_size,',').rjust(20) + "   Total files size")

        return  files_name_list


    # =======================================================================================================================
    #  COunt rhw number of files assigned to a table
    # =======================================================================================================================
    def count_files_per_table(self, status, dbms_name: str, table_name: str):
        '''
        Return the total number of files + dictionary with count per table
        '''

        counter = 0
        if table_name:
            filter = {"table": table_name}
        else:
            filter = {}     # all tables in he database

        per_table_count = {}        # Counter per table


        try:
            fs = self.cur_large
            for grid_out in fs.find(filter, no_cursor_timeout=False):
                counter = counter + 1   # Total

                index = grid_out.name.find(".")
                if index > 0:
                    table_name = grid_out.name[:index]
                    if not table_name in per_table_count:
                        per_table_count[table_name] = 1
                    else:
                        per_table_count[table_name] += 1
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error("MongoDB: Failed to count documents from DBMS '%s' and table: '%s', with error %s : %s" % (
                                                            dbms_name, table_name, str(errno), str(value)))
            counter = -1


        return  [counter, per_table_count]

    # =======================================================================================================================
    #  COunt rhw number of files assigned to a table
    # Error returns -1
    # =======================================================================================================================
    def count_files(self, status, dbms_name: str, table_name: str, one_doc:bool):

        counter = 0
        if table_name:
            filter = {"table": table_name}
        else:
            filter = {}     # all tables in he database

        try:
            fs = self.cur_large
            for grid_out in fs.find(filter, no_cursor_timeout=False):
                counter = counter + 1
                if one_doc:
                    break           # Indication of at least one doc in the collection

        except:
            errno, value = sys.exc_info()[:2]
            status.add_error("MongoDB: Failed to count documents from DBMS '%s' and table: '%s', with error %s : %s" % (
                                                            dbms_name, table_name, str(errno), str(value)))
            counter = -1


        return  counter


    # ---------------------------------------------------------------
    # drop a logical database
    # ---------------------------------------------------------------
    def drop_database(self, status, dbms_name):

        try:
            self.conn.drop_database(dbms_name)
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"MongoDB: Failed to drop database '{dbms_name}': with error: [{errno}] [{value}]")
            ret_val = False
        else:
            ret_val = True
        return ret_val

    # ---------------------------------------------------------------
    # Close a connection to the logical database
    # ---------------------------------------------------------------
    def close_connection(self, status):
        try:
            self.conn.close()
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error("MongoDB: Failed to close connection to DBMS '%s' with error %s : %s" % (self.dbms_name, str(errno), str(value)))
            ret_val = False
        else:
            ret_val = True


        return ret_val

    # ---------------------------------------------------------------
    # Test that at lead one doc assigned to the table
    # ---------------------------------------------------------------
    def is_table_exists(self, status, table_name):

        return self.count_files(status, self.dbms_name, table_name, True) == 1

