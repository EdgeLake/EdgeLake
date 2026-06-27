"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import datetime
import json
import time

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
from edge_lake.dbms.bucket_database import bucket_storage
import sys
import os
import edge_lake.dbms.akave as akave
import edge_lake.generic.params as params
from edge_lake.generic import utils_data
import edge_lake.generic.streaming_data as streaming_data

class BUCKET(bucket_storage):
    def __init__(self, bucket_info):
        super().__init__()
        self.dbms_name = bucket_info.get("dbms_name")
        self.engine_name = bucket_info.get("provider")
        self.access_key = bucket_info.get("access_key")
        self.secret_key = bucket_info.get("secret_key")
        self.region = bucket_info.get("region")
        self.ip_port = bucket_info.get("endpoint")
        self.network_id = bucket_info.get("network_id")
        self.bucket_name = bucket_info.get("bucket_name")
        self.storage_type = "Bucket Persistent"
        self.config = ""

    # --------------------------------------------------------------------------------------
    # Test that the bucket group exists
    # --------------------------------------------------------------------------------------
    def connect_to_db(self, status, user, passwd, host, port_val, dbms_name, conditions):
        """
        Connect to Bucket provider and see that bucket name exists
        """
        # Check if bucket provider is supported
        ret_val = process_status.SUCCESS
        if self.engine_name.lower() == "akave":
            # init akave object
            self.dbms = akave.AkaveConnector(self.ip_port, self.region, self.access_key, self.secret_key)
        else:
            status.add_error(
                f"The specified bucket provider {self.engine_name} is not supported.")
            return False

        # Attempt to connect to provider and create bucket if the bucket does not exist
        # If the bucket exists, then proceed.
        if not ret_val:
            if self.engine_name.lower() == "akave":
                # Check bucket connection
                ret_val, reply = self.dbms.list_buckets(status)
                if ret_val:
                    ret_val = process_status.Failed_bucket_connect
                    status.add_error(
                        f"Failed to connect to {self.engine_name} at {self.ip_port} and Region {self.region}")
                    return False
                else:
                    # init unique bucket name
                    db_name = dbms_name[6:] if dbms_name.startswith("blobs_") else dbms_name # can't have underscore in bucket name
                    node_name = params.get_param("node_name") # get node unique name
                    self.node_bucket_name = f"{db_name}-{node_name}-{self.network_id}" # define unique bucket name
                    # check if bucket does not exist. Otherwise, continue.
                    if self.node_bucket_name not in reply:
                        ret_val, reply = self.dbms.create_bucket(status, self.node_bucket_name)

            if ret_val:
                status.add_error(f"Failed to declare {self.engine_name} bucket at {self.ip_port} and Region {self.region} as the bucket name is not unique within the global namespace.")
                ret_val = process_status.Failed_bucket_create
                return False # not connected

        return True


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
        config_str = f"Node Unique Bucket Name: {self.node_bucket_name}"
        return config_str

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
        from edge_lake.dbms.db_info import _query_file_list
        ret_val = process_status.SUCCESS

        db_name = dbms_name[6:] if dbms_name.startswith("blobs_") else dbms_name
        file_name_copy = file_name
        if file_name.startswith(f"{table_name}."):
            file_name = file_name.split(".", 1)[1]

        # if the data comes directly from the `file store` command, insert it via rest so that the PSQL metadata table is also updated
        if os.path.normpath(file_path) != os.path.normpath(params.get_param("blobs_dir")): # norm path so directory with trailing dir/my_folder == dir/my_folder/
            file_path = os.path.expanduser(os.path.join(file_path,file_name_copy))

            ## Check if the file is already stored inside the table, then upload it without adding a new row to DBMS
            file_list = _query_file_list(status, db_name, table_name, archive_date, file_name, None,0)
            if not isinstance(file_list, list):
                file_list = utils_io.str_to_json(file_list)
            if isinstance(file_list, list):


                try:
                    last_update = file_list[-1]
                    if isinstance(last_update, dict):
                        # if the most recent update to this file is not deleted, then upload the file w/o adding row to dbms
                        if last_update.get("is_deleted") == "0":
                            key = f"{table_name}/{file_name}"
                            ret_val, reply = self.dbms.upload_file(status, self.node_bucket_name, file_path, key)
                            return ret_val
                except:
                    # we want to upload file and add row to DBMS
                    pass

                # upload file and add new row to DBMS
                wall_now = time.time()
                wall_dt = datetime.datetime.fromtimestamp(wall_now, tz=datetime.timezone.utc)
                timestamp = wall_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

                # get file size
                file_size = utils_io.get_file_size(status, file_path)
                # get file name
                dir_path, file_name, file_type = utils_io.extract_path_name_type(file_path)
                filename = f"{file_name}.{file_type}"

                descriptor_json = {
                    "file": filename,
                    "file_size": file_size,
                    "timestamp": timestamp,
                    "is_deleted": 0
                }

                file_data = json.dumps(descriptor_json)

                if not blob_hash_value:
                    blob_hash_value = utils_data.get_string_hash('md5', file_data,
                                                            dbms_name + table_name)  # Get the Hash of the data that is written to a file

                ret_val = self.put_from_rest(status, file_path, db_name, table_name, blob_hash_value,
                                                      archive_date, file_data)
        else:

            if struct_fname:
                # The dbms name and the table name are in the file name
                # This is the case of data pushed to AnyLog using REST or MQTT and the data is mapped using a policy
                # this process extends the file name with metadata (dbms.table)
                # Or the user in the CLI provided a file name in that format
                full_path = os.path.expanduser(
                    os.path.expandvars(os.path.join(file_path, f"{db_name}.{table_name}.{file_name}")))
            else:
                # Using the CLI - a file name is specified
                full_path = os.path.expanduser(os.path.expandvars(os.path.join(file_path, file_name)))

            # if file DNE print error message & return None as to not continue
            if not os.path.isfile(full_path):
                message = f'{self.engine_name} (store file): Failed to locate blob file: {file_name} - unable to continue'
                ret_val = process_status.Wrong_path
            else:
                key = f"{table_name}/{file_name}"
                if not self.is_duplicate(status, key):
                    ret_val, reply = self.dbms.upload_file(status, self.node_bucket_name, full_path, key)
                else:
                    if ignore_duplicate == False:
                        # Duplicates do ot return an error - allowing multiple images to point to the same file
                        message = f'{self.engine_name} (store file): Duplicate file name {file_name} in DBMS {dbms_name} in Table {table_name}'
                        ret_val = process_status.Already_exists  # We have a unique index on file_name
                        status.add_error(message)

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
        dest_file = None
        if "_id" in db_filter or "filename" in db_filter:
            one_file = True     # These are unique keys - only one file retrieved
        else:
            one_file = False     # multiple files retrieved

        if dest_type == "file" and not one_file:
            status.add_error(
                "Bucket process error: Retrieve multiple files requires a folder as a destination - a file name was provided")
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

                    table_name = db_filter.get("table")
                    archive_date = db_filter.get("archive_date")
                    query_res = db_filter.get("file_list", [])
                    download_failed = []
                    if query_res:
                        for file in query_res:
                            file = file.get("file")
                            file_key = f"{table_name}/{file}"
                            file_name = f"{dbms_name}.{table_name}.{file}"
                            ret_val, reply = self.dbms.download_file(status, self.node_bucket_name, file_key, file_name, dest_folder)
                            if ret_val:
                                download_failed.append(file_key)
                        if download_failed:
                            status.add_error(f"Failed to download files from {self.engine_name}: {download_failed}")
                    else:
                        ret_val = process_status.Failed_bucket_list
                elif one_file:

                    file_path, file_name, file_type = utils_io.extract_path_name_type(destination_name)

                    if db_filter and "filename" in db_filter:
                        file_name = db_filter["filename"]
                        index = file_name.find('.')     # Find the bucket name (equivalent to the table name)
                        if index <= 0 or index == len(file_name) - 1:
                            status.add_error(f"Wrong file name - unable to identify bucket name in the file name: '{file_type}'")
                            ret_val = process_status.Wrong_bucket_name
                        else:
                            # Extract the first part (separated by `.`) of the file name contains the table name
                            table_name, file = file_name.split(".", 1)
                            file_key = f"{table_name}/{file}"

                            if file in destination_name:
                                dest_dir, dest_file, file_type = utils_io.extract_path_name_type(destination_name)
                                if file_type:
                                    dest_file += f".{file_type}"
                            elif dest_file and dest_type == 'file':
                                # if true then use defined dest_file as it's a http stream request
                                dest_dir, dest_file, file_type = utils_io.extract_path_name_type(destination_name)
                                dest_file = f"{dest_file}.{file_type}"
                            else:
                                dest_dir = file_path
                                dest_file = f"{dbms_name}.{file_name}"

                            ret_val, reply = self.dbms.download_file(status, self.node_bucket_name, file_key, dest_file, dest_dir)

                    else:
                        # Get the prefix
                        file_prefix = db_filter["_id"]
                        # Convert extract the table name and file_hash
                        table_name, file_hash = file_prefix.split(".", 1)
                        # Define prefix
                        prefix = f"{table_name}/{file_hash}"
                        # Find the filename given the hash as the prefix
                        file_list = self.dbms.list_files(status, self.node_bucket_name, prefix)
                        # Extract file key given prefix
                        file_key = ""
                        try:
                            # extract latest file based on IsLatest
                            file_keys = file_list[1]
                            for k in file_keys:
                                if k.get("IsLatest"):
                                    file_key = k
                        except: # note this exception should never trigger because an empty prefix will return file_list[0][1] = ""
                            errno, value = sys.exc_info()[:2]
                            status.add_error(f"Bucked download error: missing filename to download: {errno} : {value}")
                            ret_val = process_status.FILE_DOES_NOT_EXISTS
                        if not ret_val and file_key:
                            # Extract filename
                            dest_dir, dest_file, file_type = utils_io.extract_path_name_type(destination_name)
                            # if filename is provided use provided
                            if dest_file and file_type:
                                dest_file = f"{dest_file}.{file_type}"
                            else: # use the file key
                                dest_file = file_key.replace("/", ".")
                            # Get file version ID
                            version_id = file_key.get("VersionId")
                            key = file_key.get("Key")
                            # Download file
                            ret_val, reply = self.dbms.download_file(status, self.node_bucket_name, key, dest_file,
                                                                     dest_dir, version_id)
                else:
                    status.add_error("Destination folder for output files from bucket are not properly defined")
                    ret_val = process_status.ERR_command_struct

            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(
                    f"Bucket process error: Failed to retrieve documents from DBMS '{dbms_name}' and search by: '{db_filter}', with error {errno} : {value}")
                ret_val = process_status.DBMS_error

        return ret_val

   # =======================================================================================================================
    #  Write via REST call that provides the data with the HTTP -F option
    # the file is provided via REST in the buffer using: curl -X POST -H "command: file store where dbms = admin and table = files and dest = file_rest " -F "file=@testdata.txt" http://10.0.0.78:7849
    # ======================================================================================================================
    def put_from_rest(self, status, file_name, dbms_name, table_name, blob_hash_value, archive_date, blob_descriptor=None):

        full_file_path = os.path.expanduser(os.path.expandvars(file_name))

        # copy file to blobs_dir
        file_dir, fname, file_type = utils_io.extract_path_name_type(full_file_path)
        # Filename cannot contain dbms and table
        db_name = f"blobs_{dbms_name}" if not dbms_name.startswith("blobs_") else dbms_name
        prefix = f"{db_name}.{table_name}."
        if fname.startswith(prefix):
            fname = fname.split(prefix)[1]
        blobs_dir = f'{params.get_param("blobs_dir")}/{dbms_name}.{table_name}.{fname}.{file_type}'
        ret_val = utils_io.copy_file(file_name, blobs_dir)
        if ret_val:
            bwatch_dir = params.get_param("bwatch_dir") # get !bwatch_dir
            prep_dir = params.get_param("prep_dir") # get !prep_dir
            error_dir = params.get_param("err_dir") # get !err_dir

            new_f_name = f"{fname}"

            ret_val = streaming_data.write_by_prep_move(status, prep_dir, bwatch_dir, error_dir, dbms_name, table_name, new_f_name, blob_hash_value, '0', None, blob_descriptor, "json")

        return ret_val

    # =======================================================================================================================
    #  Delete a file from storage
    # content = self.cur_large.put(file_data, filename=per_table_name, _id = per_table_hash, table = table_name, archive_date = archive_date)
    # =======================================================================================================================

    def remove_file(self, status, dbms_name, table_name, id_str, hash_value):

        ret_val = process_status.SUCCESS

        file_key = table_name + '/' + id_str
        try:
            if id_str or hash_value:
                ret_val, reply = self.dbms.delete_file(status, self.node_bucket_name, file_key, None)
        except Exception as error:
            status.add_error(f'Bucket: Failed to locate content for file with id {id_str} in {self.engine_name} (Error: {error})')
            ret_val = process_status.ERR_process_failure

        return ret_val

    # =======================================================================================================================
        #  Get files list
        #  get files where dbms = blobs_edgex and table = image
        # =======================================================================================================================

    def get_file_list(self, status, dbms_name: str, table_name: str, id_str: str, hash_val: str, archive_date: str,
                      limit: int):

        files = None
        if hash_val:
            status.add_error(
                "Bucket search by file hash is not currently supported")

        else:
            if not id_str: # if id_str not specified or None make it empty string
                id_str = ""
            # prefix = f"{table_name}/{id_str}"
            prefix = id_str.split('.', 1)
            prefix = '/'.join(prefix)
            files_name_list = self.dbms.list_files(status, self.node_bucket_name, prefix)
            files = []
            if files_name_list:
                file_list = files_name_list[1]
                for file in file_list:
                    if self.engine_name == "akave":
                        if file.get("IsLatest"):
                            files.append({"table_name": table_name, "file": file.get("Key").split("/")[1]})
        if limit > 0:
            return files[:limit]
        else:
            return files

    # ---------------------------------------------------------------
    # Close a connection to the logical database
    # ---------------------------------------------------------------
    def close_connection(self, status, db_connect):
        try:
            del self.dbms
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Bucket process error: Failed to close connection to a bucket considered as DBMS '{self.dbms_name}' with error {errno} : {value}")
            ret_val = False
        else:
            ret_val = True

        return ret_val

    # ---------------------------------------------------------------
    # drop a logical database
    # This Bucket object needs to be dropped before disconnecting from
    # the DBMS.
    # ---------------------------------------------------------------
    def drop_database(self, status, dbms_name):
        ret_val, reply = self.dbms.delete_bucket(status, self.node_bucket_name, True) # delete all files and node bucket
        return ret_val

    # =======================================================================================================================
    #  Count row number of files assigned to a table
    # =======================================================================================================================
    def count_files_per_table(self, status, dbms_name: str, table_name: str):
        '''
        Return the total number of files + dictionary with count per table
        '''
        db_name = f"blobs_{dbms_name}" if not dbms_name.startswith("blobs_") else dbms_name

        # TODO: Note we can also query the PSQL database directly and not have a dependency on Akave

        per_table_count = {} # Counter per table

        counter = 0
        if not table_name:
            ret_val, tables = self.dbms.get_unique_tables(status, self.node_bucket_name)
            if ret_val:
                return None
        else:
            tables = [table_name]

        for table in tables:
            # query file list for the table
            file_list = self.get_file_list(status, dbms_name, table, "", "", None, 0)

            if file_list:
                table_count = len(file_list)
            else:
                table_count = 0
            per_table_count[table] = table_count
            counter += table_count

        return counter, per_table_count


    def get_database_tables(self, status, d_name):
        ret_val, tables = self.dbms.get_unique_tables(status, self.node_bucket_name)
        if ret_val:
            status.add_error(f"Unable to identify tables in {self.engine_name} Bucket {self.node_bucket_name}")
        return ret_val, tables

    def is_duplicate(self, status, key):
        ret_val, exists = self.dbms.key_exists(status, self.node_bucket_name, key)
        return exists

