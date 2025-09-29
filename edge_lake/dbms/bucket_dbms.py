"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
from edge_lake.dbms.database import sql_storage
import edge_lake.dbms.bucket_store as bucket_store
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import sys

class BUCKET(sql_storage):
    def __init__(self, bucket_group):
        self.bucket_group = bucket_group
        self.dbms_name = ""
        self.engine_name = ""
        self.ip_port = ""
        self.storage_type = "bucket"
        self.config = ""

    # --------------------------------------------------------------------------------------
    # Test that the bucket group exists
    # --------------------------------------------------------------------------------------
    def connect_to_db(self, status, user, passwd, host, port_val, dbms_name, conditions):
        is_connected = bucket_store.is_group_declared(self.bucket_group)
        if not is_connected:
            status.add_error(f"Failed to connect a bucket as a DBMS - the bucket group '{self.bucket_group}' is not declared")
        else:
            self.dbms_name = dbms_name
        return is_connected


    # =======================================================================================================================
    #  SETUP Calls - These are issued when the database is created
    # =======================================================================================================================

    def exec_setup_stmt(self, status):
        return True

    # =======================================================================================================================
    #   #  Returns True if the engine API manage pool of connections
    # =======================================================================================================================
    def with_connection_pool(self):
        return False

    # =======================================================================================================================
    #   #  Returns True if the engine API supports SQL
    # =======================================================================================================================
    def is_sql_storage(self):
        return False

    # =======================================================================================================================
    #   #  Returns the Bucket Group
    # =======================================================================================================================
    def get_config(self, status):
        return self.bucket_group

        # =======================================================================================================================
        #  Update Mongo with a file
        # =======================================================================================================================

    def store_file(self, status, dbms_name: str, table_name: str, file_path: str, file_name: str, blob_hash_value: str,
                   archive_date: str, ignore_duplicate: bool, struct_fname: bool, trace_level: int):
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
            struct_fname - dbms.table are in the file name
        :params:
            status:bool
            full_path:str - file path
        :return:
            status
        """

        db_name = dbms_name[6:] if dbms_name.startswith("blobs_") else dbms_name
        complete_file_name = f"{db_name}.{table_name}.{blob_hash_value}"

        key = f"{dbms_name}/{table_name}/{blob_hash_value}" # Note that bucket keys do not support "."
        cmd_words = ["bucket", "file", "upload", "where", "group", "=", self.bucket_group, "and", "name", "=", table_name, "and", "source_dir", "=", file_path, "and", "file_name", "=", complete_file_name, "and", "key", "=", key]

        # bucket file upload where group = my_group and name = my_bucket and source_dir = my_dir and file_name = file_name
        # bucket file upload where group = my_group and name = my-bucket and source_dir = /Users/roy and file_name = test.txt and key = dir1/test2.txt

        ret_val, dummy = bucket_store.file_upload(status, None, cmd_words, 0)

        return ret_val

    # =======================================================================================================================
    #  file retrieve to a destination file
    # =======================================================================================================================
    def retrieve_files(self, status, dbms_name: str, db_filter: dict, limit: int, dest_type: str,
                       destination_name: str):
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
        dest_folder = ""
        db_name = dbms_name[6:] if dbms_name.startswith("blobs_") else dbms_name

        if "_id" in db_filter or "filename" in db_filter:
            one_file = True     # These are unique keys - only one file retrieved
        else:
            one_file = False     # multiple files retrieved

        if dest_type == "file" and not one_file:
            status.add_error(
                "Akave: Retrieve multiple files requires a folder as a destination - a file name was provided")
            ret_val = process_status.ERR_command_struct

        elif dest_type == "file":
            dest_file = destination_name
        else:
            dest_file = None
            dest_folder = destination_name

        if not ret_val:
            try:
                if not one_file and dest_folder: # request to download multiple files
                    # Get list of files satisfying db_filter
                    get_file_list_cmd = ["get", "bucket", "files", "where", "group", "=", self.bucket_group, "and",
                                         "name", "=", db_name, "and", "prefix", "=", db_filter, "and", "format",
                                        "=", "json"]

                    ret_val, file_list = bucket_store.get_files(status, None, get_file_list_cmd, 0)

                    for file_name in file_list:
                        download_file_cmd_words = ["bucket", "file", "download", "where", "group", "=", self.bucket_group, "and",
                                     "name", "=", db_name, "and", "key", "=", file_name, "and", "dest_dir", "=", dest_folder,
                                     "and", "file_name", "=", file_name]
                        ret_val, reply = bucket_store.file_download(status, None, download_file_cmd_words, 0)

                elif one_file:

                    file_path, file_name, file_type = utils_io.extract_path_name_type(destination_name)

                    if db_filter and "filename" in db_filter:
                        file_name = db_filter["filename"]
                        index = file_name.find('.')     # Find the bucket name (equivalent to the table name)
                        if index <= 0 or index == len(file_name) - 1:
                            status.add_error(f"Wrong file name - unable to identify bucket name in the file name: '{file_type}'")
                            ret_val = process_status.Wrong_bucket_name
                        else:
                            # Bucket name is the prefix for the file name
                            bucket_name = file_name[:index]
                            file_key = f"{dbms_name}/{bucket_name}/{file_name[index + 1:]}"

                            if destination_name:
                                dest_dir, dest_file, file_type = utils_io.extract_path_name_type(destination_name)
                                if file_type:
                                    dest_file += f".{file_type}"
                            else:
                                dest_dir = file_path
                                dest_file = file_name

                            download_file_cmd_words = ["bucket", "file", "download", "where", "group", "=", self.bucket_group, "and",
                                         "name", "=", bucket_name, "and", "key", "=", file_key, "and", "dest_dir", "=", dest_dir,
                                         "and", "file_name", "=", dest_file]
                            ret_val, reply = bucket_store.file_download(status, None, download_file_cmd_words, 0)
                    else:
                        status.add_error("Bucked download error: missing filename to download")
                        ret_val = process_status.ERR_command_struct

                else:
                    status.add_error("Destination folder for output files from bucket are not properly defined")
                    ret_val = process_status.ERR_command_struct

            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(
                    f"Akave: Failed to retrieve documents from DBMS '{dbms_name}' and search by: '{db_filter}', with error {errno} : {value}")
                ret_val = process_status.DBMS_error

        return ret_val

   # =======================================================================================================================
    #  Write via REST call that provides the data with the HTTP -F option
    # the file is provided via REST in the buffer using: curl -X POST -H "command: file store where dbms = admin and table = files and dest = file_rest " -F "file=@testdata.txt" http://10.0.0.78:7849
    # ======================================================================================================================
    def put_from_rest(self, status, file_name, dbms_name, table_name, blob_hash_value, archive_date ):

        ret_val = process_status.SUCCESS
        return ret_val

    # =======================================================================================================================
    #  Delete a file from storage
    # content = self.cur_large.put(file_data, filename=per_table_name, _id = per_table_hash, table = table_name, archive_date = archive_date)
    # =======================================================================================================================

    def remove_file(self, status, dbms_name, table_name, id_str, hash_value):

        ret_val = process_status.SUCCESS

        if hash_value:
            # Search by hash value
            id_filter = {"_id": table_name + '.' + hash_value}
        else:
            # Search by name
            id_filter = {"filename": table_name + '.' + id_str}

        file_key = table_name + '.' + id_str
        try:
            get_file_list_cmd = ["get", "bucket", "files", "where", "group", "=", self.bucket_group, "and",
                                 "name", "=", dbms_name, "and", "prefix", "=", file_key, "and", "format",
                                 "=", "json"]

            ret_val, file_list = bucket_store.get_files(status, None, get_file_list_cmd, 0)
        except Exception as error:
            status.add_error(f'MongoDB: Failed to locate content for file with id {id_str} (Error: {error})')
            ret_val = process_status.ERR_process_failure
        else:
            if file_list:
                try:
                    cmd_words = ["bucket", "file", "delete", "where", "group", "=", self.bucket_group, "and", "name",
                                 "=", dbms_name, "and",
                                 "key", "=", file_key]
                    ret_val, reply = bucket_store.file_delete(status, None, cmd_words, 0)
                except Exception as error:
                    status.add_error(f'Akave: Failed to remove file with key {file_key} (Error: {error})')
                    ret_val = process_status.File_read_failed
            else:
                ret_val = process_status.No_file_in_storage

        return ret_val

    # =======================================================================================================================
        #  Get files list
        #  get files where dbms = blobs_edgex and table = image
        # =======================================================================================================================

    def get_file_list(self, status, dbms_name: str, table_name: str, id_str: str, hash_val: str, archive_date: str,
                      limit: int):

        files_name_list = []
        return files_name_list