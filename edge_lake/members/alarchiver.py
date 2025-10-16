"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.generic.interpreter as interpreter
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_print as utils_print
import edge_lake.dbms.db_info as db_info

# ------------------------------------------------------------------------------------
# Archive Documents, videos, etc
# ------------------------------------------------------------------------------------
arch_running_flag = False

status_msg_ = ""

statistics = {
    "processed" : [0,0,0,"",""],      # Info: Counter on JSON Processed: Success, Error, Last-Error, Last File Processed, error placed on status
    "blobs" : {
        # Maintain info as f(table) -> # Processed, Sucess, Error
    }
}

# ------------------------------------------------------------------------------
# Test if the data distributor is running
# ------------------------------------------------------------------------------
def is_arch_running():
    global arch_running_flag

    return arch_running_flag

# ------------------------------------------------------------------------------
# Return config info
# ------------------------------------------------------------------------------
def get_info(status):

    global status_msg_

    return status_msg_ if arch_running_flag else ""


# ------------------------------------------------------------------------------
# Read JSON files from the BWATCH dir and archive the data accordingly
# This file is written in: generic.steaming_data.write_by_prep_move
# Example: run blobs archiver where dbms = true and folder = false and compress = false and reuse_blobs = true
# ------------------------------------------------------------------------------
def data_archiver(dummy: str, conditions: dict):
    global arch_running_flag
    global statistics
    global status_msg_

    status = process_status.ProcessStat()

    file_types = ["json", "backup", "gz"]  # JSON files are with the Source Data and Archive files are with partition data

    blockchain_file = params.get_value_if_available("!blockchain_file")

    bwatch_dir = interpreter.get_one_value_or_default(conditions, "bwatch_dir", "!bwatch_dir") + params.get_path_separator()
    blobs_dir = interpreter.get_one_value_or_default(conditions, "blobs_dir", "!blobs_dir")
    if blobs_dir[-1] != params.get_path_separator():
        blobs_dir += params.get_path_separator()
    err_dir = interpreter.get_one_value_or_default(conditions, "err_dir", "!err_dir")
    watch_dir = interpreter.get_one_value_or_default(conditions, "watch_dir", "!watch_dir")
    if watch_dir[-1] != '\\' and watch_dir[-1] != '/':
        watch_dir += params.get_path_separator()

    archive_file = conditions["folder"][0]          # archive the file with the blob data (default)

    update_dbms = conditions["dbms"][0]             # Update a dbms with the blob data
    compress_file = conditions["compress"][0]
    reuse_blobs = conditions["reuse_blobs"][0]      # True value if the file may already be in the database

    status_msg_ = f"Flags: dbms = {update_dbms}, folder = {archive_file}, compress = {compress_file}, reuse_blobs = {reuse_blobs}"

    file_info = utils_io.FileMetadata()     # A structure to manage the info from the file name

    arch_running_flag = True

    previous_mapping_policy_id = '0'

    while 1:

        file_list = []  # needs to be inside the while loop to be initiated (as the same file will be called again)

        ret_val, file_name = member_cmd.get_files_from_dir(status, "archiver", 5, bwatch_dir, "file", file_types, file_list, None, None, False)

        if ret_val:
            #  Archiver terminated - or global termination
            break

        trace_level = member_cmd.commands["run blobs archiver"]['trace']

        ret_val = file_info.set_file_name_metadata(status, file_name)

        if ret_val:
            status.add_keep_error("Archiver failed to retrieve dbms name and table name from file: '%s'" % file_name)
            ret_val = process_status.Error_file_name_struct
        else:
            dbms_name = file_info.get_dbms_name()
            table_name = file_info.get_table_name()
            stat_key = f"{dbms_name}.{table_name}"
            mapping_policy_id = file_info.get_instructions()
            file_column = "file"    # A default name for a column that caries the hash value of the file
            file_name_prefix = f"{dbms_name}.{table_name}."

            if mapping_policy_id != '0':
                # get the policy
                if mapping_policy_id != previous_mapping_policy_id:
                    ret_val, policies_out = member_cmd.blockchain_retrieve(status, blockchain_file, "get", "mapping", '{"id" : "%s"}' % mapping_policy_id, "")
                    previous_mapping_policy_id = mapping_policy_id      # Save to avoid reading of the same policy

                    # Get the field name that points to the image/video
                    if len(policies_out) != 1:
                        status.add_keep_error(f"Archiver retrieved multiple mapping policies with the same id: {mapping_policy_id}")
                        ret_val = process_status.ERR_wrong_json_structure
                    elif not "mapping" in policies_out[0]:
                        status.add_keep_error(f"Archiver failed to retrieved a mapping policy with id: {mapping_policy_id}")
                        ret_val = process_status.ERR_wrong_json_structure
                    else:
                        mapping_policy = policies_out[0]["mapping"]
                        if not "schema" in mapping_policy:
                            status.add_keep_error(f"Archiver process failed: Mapping Policy with id: {mapping_policy_id} has no schema")
                            ret_val = process_status.ERR_wrong_json_structure
                        else:
                            schema = mapping_policy["schema"]
                            for column_name, info in schema.items():
                                if isinstance(info, dict) and "hash" in info:
                                    # This is the column that represents the Image/Video file
                                    file_column = column_name
                                    break

            if not ret_val:



                # Read the JSON files and archive the data accordingly
                ret_val, blobs_list = load_and_test_json(status, file_info, blobs_dir, file_column, file_name_prefix, reuse_blobs)


        if not ret_val:
            # The attributes used were validated at load_and_test_json
            if not stat_key in statistics:
                statistics[stat_key] = [0,0,0, "",""]    # Success, Failure, last Error, last blob file


            # Process - move the blob and create a JSON for the WATCH dir

            utc_time = utils_columns.get_current_utc_time("%Y-%m-%dT%H:%M:%S.%fZ")
            date_time_key = utils_io.utc_timestamp_to_key(utc_time)

            sql_description = ""        # Create the data that will be paced on the watch dir

            for json_entry in blobs_list:

                blob_hash_value = json_entry[file_column]    # It was tested to exist in load_and_test_json()
                # Find if hash value represents file name + type

                statistics[stat_key][3] = blob_hash_value

                file_id_name = file_name_prefix + blob_hash_value  # database + table + source + name

                absolute_name = blobs_dir + file_id_name

                file_exists = True
                if reuse_blobs:
                    # Test if the file exists on disk - as the file may be in the database because of previous entry
                    if not utils_io.is_path_exists(absolute_name):  # Test if blob file exists
                        file_exists = False     # File not in folder

                if file_exists:
                    if update_dbms:
                        # UPDATE THE DBMS
                        ret_val = db_info.store_file(status,  "blobs_" + dbms_name, table_name, blobs_dir, blob_hash_value, blob_hash_value, date_time_key[:6], True, True, trace_level)
                        if ret_val:
                            break


                    if archive_file:
                        # Move the file to archive + compress -> if archiving fails - move to err_dir
                        ret_val = utils_io.archive_file(status, "*", blobs_dir, err_dir, absolute_name, False, date_time_key)
                        if ret_val:
                            status.add_keep_error("Blobs archiver is configured to archive blobs to folder: '%s' Failed to archive file '%s'" % (blobs_dir, file_id_name))
                            break

                    elif update_dbms:
                        # The file was updated to the DBMS without error
                        # Delete the blob file
                        if not utils_io.delete_file(absolute_name, True):
                            status.add_keep_error("Archiver: Failed to delete file: '%s'" % absolute_name)
                            ret_val = process_status.Failed_to_delete_file
                            break


                new_row = utils_json.to_string(json_entry)
                if not new_row:
                    status.add_keep_error("Failed to generate info for SQL tables for blob file: %s" % file_name)
                    break
                sql_description += (new_row + "\n")
                statistics[stat_key][0] += 1  # Count Success

            if not ret_val:
                # write the file and copy to watch dir
                hash_value = utils_data.get_string_hash('md5', sql_description, dbms_name + '.' + table_name)
                # File name is with 0 for the mapping policy as the data was mapped in the mqtt client process
                table_file_name =  "%s%s.%s.json" % (file_name_prefix, hash_value, '0') # The new file to be written
                ret_code = utils_io.write_str_to_file(status, sql_description, bwatch_dir + table_file_name) # The file to be updated to the SQL dir
                if not ret_code:
                    ret_val = process_status.File_write_failed
                    status.add_keep_error("Archiver failed to write file: %s" % table_file_name)
                    break
                # Move file to the watch directory
                # [dbms name].[table name].[data source].[hash value].[mapping policy].[TSD member ID].[TSD row ID].[TSD date].json
                operator_file_name = "%s0.%s.%s.json" % (file_name_prefix, hash_value, mapping_policy_id) # The
                ret_code = utils_io.rename_file(status, bwatch_dir + table_file_name, watch_dir + operator_file_name)
                if not ret_code:
                    ret_val = process_status.ERR_dir_does_not_exists
                    status.add_keep_error("Archiver failed to write file: %s" % table_file_name)
                    break
            else:
                statistics[stat_key][1] += 1  # Count Error
                statistics[stat_key][2] = ret_val
                statistics[stat_key][4] = status.get_saved_error()

        if ret_val:
            statistics["processed"][1] += 1  # Count files Failed
            statistics["processed"][2] = ret_val
            statistics["processed"][3] = file_info.get_no_path_name()  # Save the JSON file name being processed (without the path)
            statistics["processed"][4] = status.get_saved_error()

            # Move file to error dir
            if not utils_io.file_to_dir(status, err_dir, file_name, "err_%u" % ret_val, True):
                break
        else:
            statistics["processed"][0] += 1  # Count files processed successfully
            if not utils_io.delete_file(file_name, True):
                status.add_keep_error("Archiver: Failed to delete file: '%s'" % file_name)
                ret_val = process_status.Failed_to_delete_file
                break

    process_log.add_and_print("event", "Archiver process terminated: %s" % process_status.get_status_text(ret_val))
    arch_running_flag = False


# ------------------------------------------------------------------------------
# Test the structure of the JSON object representing the blobs
# Example file:
'''
{ "blobs" : {
		"dbms" : "video",
		"table" : "releases",
		"location" : "!video_dir",

		"list" : [ 
			{  
			  "info" : {
				"timestamp": "2022-05-30 16:07:15.07616",
				"title": "Big Buck Bunny",
				"minutes": 10,
				"name": "Test Video1",
				"file": "Big_Buck_Bunny.mp4"
				}
			},
		]
	}
}
'''
# ------------------------------------------------------------------------------
def load_and_test_json(status, file_info, blobs_dir, file_column, file_name_prefix, reuse_blobs):
    '''
    file_name - the name of the JSON file placed in the bwatc dir
    file_column - the name of the column with the hash value of the file
    reuse_blobs - True if the file may be in the database

    returns
    ret_val
    blobs_location - the directory where the blob data is placed
    blobs_info - the JSON object (the file data as a JSON object)
    '''


    ret_val = process_status.SUCCESS

    json_file_name = file_info.get_file_name()

    blobs_list = utils_io.read_json_strings(status, json_file_name)

    if not blobs_list:
        ret_val = process_status.Failed_to_read_files_from_dir
    else:

        # go over the list and manage the blobs
        for index, json_entry in enumerate(blobs_list):

            if not file_column in json_entry:
                # This attributes includes the hash value of the image
                status.add_keep_error("Archiver error: Missing '%s' attribute in line %u with file: '%s.bin'" % (file_column, index + 1, json_file_name))
                ret_val = process_status.ERR_wrong_json_structure
                break

            blob_hash_value = json_entry[file_column]

            if not reuse_blobs:
                # If reuse files - the file may be in the database
                absolute_name =  blobs_dir + file_name_prefix + blob_hash_value
                if not utils_io.is_path_exists(absolute_name): # Test if blob file exists
                    status.add_keep_error("Archiver error: The blob file '%s' does not exists" % (absolute_name))
                    ret_val = process_status.ERR_file_does_not_exists
                    break  # The file is missing

    return [ret_val, blobs_list]

# ------------------------------------------------------------------------------
# Return statistics based on the command: get archiver
# ------------------------------------------------------------------------------
def get_archiver(status, io_buff_in, cmd_words, trace):

    global statistics
    global arch_running_flag


    if not arch_running_flag:
        reply = "\r\nArchiver not active"
    else:

        table = "Source JSON"
        success = statistics["processed"][0]
        error = statistics["processed"][1]
        if statistics["processed"][2]:
            last_error = process_status.get_status_text(statistics["processed"][2])
        else:
            last_error = ""
        last_file = statistics["processed"][3]
        last_message = statistics["processed"][4]

        list_info = [ [table, success, error, last_error, last_file, last_message] ]

        blobs = statistics["blobs"]
        for table, info in blobs.items():
            success = info[0]
            error = info[1]
            if not info[2]:
                last_error = ""
            else:
                last_error = process_status.get_status_text(info[2])
            last_message = info[4]


            list_info.append([table, success, error, last_error, "", last_message ])


        reply = utils_print.output_nested_lists(list_info, "", ["Table/File", "Success", "Error", "Last Error", "Last File", "Last Message"], True)

    return [process_status.SUCCESS, reply]
