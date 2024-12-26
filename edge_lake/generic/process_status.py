"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# =======================================================================================================================
# =======================================================================================================================
# Creates an object that is updated with return values and messages from a process
# =======================================================================================================================

import traceback
import threading
import sys

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.process_log as process_log
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.al_parser as al_parser
import edge_lake.job.job_handle as job_handle
import edge_lake.job.task_scheduler as task_scheduler
from edge_lake.job.job_instance import JobInfo
import edge_lake.generic.params as params

with_traceback_ = False
traceback_text_ = ""

NON_ERROR_RET_VALUE = 1000  # returned value which are not errors
CALL = 1001
GOTO = 1002
END_SCRIPT = 1003  # end current script
RETURN = 1004
RE_RUN_QUERY = 1005  # if metadata is not available metadata is requested and the query is issued again
Empty_data_set = 1006
AT_LIMIT = 1007  # rows returned match limit
TRAVERSAL_COMPLETED = 1008
ON_ERROR_GOTO = 1009
ON_ERROR_CALL = 1010
ON_ERROR_IGNORE = 1011
ON_ERROR_END_SCRIPT = 1012
ELSE_IGNORED = 1013
DO_IGNORED = 1014
FILE_EXISTS = 1015
FILE_DOES_NOT_EXISTS = 1016
EXIT_SCRIPTS = 1017       # exit all scripts
IGNORE_ENTRY = 1018     # Used in streaming_conditions() to remove an entry from the list
PROCESSED_IMMEDIATE = 1019  # Used by operator to show file and TSD updated immediately
IGNORE_ATTRIBUTE = 1020       # get the next attribute (called by a mapping policy script)
IGNORE_EVENT = 1021       # Get the next event (called by a mapping policy script)
IGNORE_SCRIPT = 1022      # STop processing the policy script
CHANGE_POLICY = 1023    # Use a different policy (from the policies that are declared as import)

control_text = [
    "",
    "CALL",  # 1001
    "GOTO",  # 1002
    "END SCRIPT",  # 1003
    "RETURN",  # 1004
    "RE RUN QUERY",  # 1005
    "Empty Data Set",  # 1006
    "AT LIMIT",  # 1007
    "TRAVERSAL COMPLETED",  # 1008
    "ON ERROR GOTO",  # 1009
    "ON ERROR CALL",  # 1010
    "ON ERROR IGNORE",  # 1011
    "ON ERROR END SCRIPT",  # 1012
    "ELSE IGNORED",  # 1013
    "DO IGNORED",  # 1014
    "File exists",   # 1015
    "File does not exists",  # 1016
    "Exit all scripts",     # 1017
    "Remove streaming entry", # 1018
    "Processed with immediate", # 1019
    "Ignore Attribute",    # 1020
    "Ignore Event",       # 1021
    "Ignore Script",       # 1022
    "Use a different policy",       # 1023
]

JOB_INSTANCE_NOT_USED = -1

EXIT = -1
SUCCESS = 0
ERR_unrecognized_command = 1  # unrecogized command line
ERR_command_struct = 2  # command recognized but error in params
ERR_process_failure = 3
ERR_io_error = 4
ERR_dbms_name = 5
ERR_wrong_json_structure = 6
ERR_wrong_data_structure = 7
No_reply = 8
ERROR_DATA_SIZE_LARGER_THAN_BUFFER = 9
ERR_dbms_not_opened = 10
ERR_missing_brackets = 11
ERR_file_does_not_exists = 12
Compare_failure = 13
File_copy_failed = 14
ERR_network = 15
ERR_SQL_failure = 16
Missing_metadata_info = 17
SQL_not_supported = 18
BLOCKCHAIN_not_recognized = 19
ERR_dest_IP_Ports = 20
ERR_source_IP_Ports = 21
Directory_already_in_watch = 22
ERR_job_id = 23
DBMS_error = 24
No_data_with_input_query = 25
TIMEOUT = 26
ERR_json_to_insert = 27
ERR_table_name = 28
No_metadata_info = 29
BLOCKCHAIN_operation_failed = 30
ERR_place_data_in_watch_dir = 31
ERR_place_data_in_rest_dir = 32
Missing_configuration = 33
Failed_python_call = 34
ERR_dir_does_not_exists = 35
ERR_write_stream = 36
JOB_not_active = 37
ERR_failed_to_send_err_msg = 38
ERR_operator_node = 39
Error_command_params = 40
DBMS_unexpected_reply = 41
DBMS_not_open_or_table_err = 42
ERR_connected_dbms_ip_port = 43
File_move_failed = 44
Failed_load_table_blockchain = 45
Failed_to_parse_sql = 46
File_open_failed = 47
File_write_failed = 48
DBMS_connection_error = 49
DBMS_NOT_COMNNECTED = 50
REST_call_err = 51
Failed_INSERT = 52
Failed_UPDATE = 53
Missing_event_script = 54
Arguments_mismatch = 55
Uncovered_err = 56
The_thread_has_no_script = 57
No_local_blockchain_file = 58
Empty_Local_blockchain_file = 59
Failed_to_execute_bring = 60
System_call_error = 61
Failed_to_extract_file_components = 62
Directory_not_writeable = 63
Missing_file_name = 64
Compression_failed = 65
Table_struct_not_available = 66
Failed_to_analyze_json = 67
Failed_to_pull_blockchain = 68
Wrong_blockchain_struct = 69
Duplicate_object_id = 70
Failed_to_delete_file = 71
Failed_to_rename_file = 72
Missing_operators_for_table = 73
Watch_dir_not_defined = 74
Inconsistent_declarations = 75
QUERY_TIME_ABOVE_LIMIT = 76
QUERY_VOLUME_ABOVE_LIMIT = 77
Wrong_path = 78
Missing_par_info = 79
Drop_partition_failed = 80
Connection_error = 81
Table_without_partition_def = 82
Missing_dbms_name = 83
Input_query_not_available = 84
Failed_to_retrieve_instructions = 85
Duplicate_JSON_File = 86
Wrong_policy_structure = 87
Wrong_username_passwd = 88
Email_message_failed = 89
Failed_to_read_files_from_dir = 90
Missing_password = 91
Failed_to_create_dir = 92
Wrong_password = 93
Duplicate_signatures = 94
Missing_public_key = 95
Internal_block_error = 96
Failed_message_authentication = 97
Failed_message_sign = 98
Failed_to_import_lib = 99
Decompression_failed = 100
Wrong_dbms_key_in_delete = 101
User_name_exists = 102
Failed_to_add_new_user = 103
Wrong_dbms_type = 104
Scheduler_not_active = 105
Backup_failed = 106
Subset_of_policies_deleted = 107
No_resources = 108
Blockchain_table_not_local = 109
REST_header_err = 110
Drop_DBMS_failed = 111
Missing_script = 112
Needed_policy_not_available = 113
Incomplete_policy = 114
Wrong_file_type = 115
File_time_not_accessible = 116
TSD_not_available = 117
Missing_parent_policy = 118
Error_file_name_struct = 119
Empty_result_set = 120
TSD_process_failed = 121
MQTT_connect_failure = 122
MQTT_publish_failed = 123
MQTT_info_err = 124
MQTT_data_err = 125
MQTT_err_dbms_name = 126
MQTT_err_table_name = 127
MQTT_wrong_client_id = 128
File_move_to_watch_failed = 129
Table_excluded = 130
File_from_unrecognized_member = 131
Local_table_not_in_blockchain = 132
Failed_host_name = 133
ERR_in_select_stmt = 134
TCP_not_running = 135
Process_already_running = 136
Failed_to_generate_keys = 137
Uninitialized_variable = 138
user_auth_disabled = 139
File_read_failed = 140
Not_in_commands_dict = 141          # Only to be used in one place - same printout as ERR_unrecognized_command
Private_key_not_available = 142
Encryption_failed = 143
Decryption_failed = 144
Authentication_failed = 145
Failed_to_extract_msg_components = 146
No_tsd_tables = 147
Move_to_archive_failed = 148
None_unique_ip_port = 149
Sync_not_running = 150
Policy_not_in_local_file = 151
No_files_to_copy = 152
Err_dir_name = 153
MQTT_non_subscribed_err = 154
Failed_to_determine_dest = 155
Wrong_http_metod = 156
MQTT_server_error = 157
MQTT_not_in_json = 158
Unrecognized_mqtt_topic = 159
Err_in_broker_msg_format = 160
Err_output_format = 161
Task_not_in_scheduler = 162
Unknown_user_agent = 163
No_monitored_info = 164
Failed_query_process = 165
Missing_leading_results = 166
Missing_data_ingest_mapping = 167
JOB_terminated_by_caller = 168
Command_not_executed = 169
Missing_id_in_policy = 170
Operator_not_allowed_with_table = 171
Files_not_available_in_dir = 172
ERR_timezone = 173
No_cluster_assigned = 174
Wrong_key_value = 175
Err_test_file_struct = 176
Failed_kafka_process = 177
SQL_not_executed = 178
Connected_to_different_dbms = 179
Non_unique_policy_values = 180
Failed_to_parse_if = 181
Error_timeout_val = 182
Missing_blob_file_name = 183
Missing_key_in_dictionary = 184
Failed_blob_insert = 185
Failed_blob_retrieve = 186
Failed_to_list_files = 187
Data_type_not_supported = 188
No_file_in_storage = 189
No_dbms_engine_support = 190
Blobs_dir_does_not_exists = 191
Operator_not_enabled = 192
Wrong_header_struct = 193
CASTING_FAILURE = 194
NON_supported_casting = 195
Failed_to_encode_data = 196
Duplicate_condition = 197
Already_exists = 198
Encoding_failed = 199
Decoding_failed = 200
Commit_failed = 201
Rollback_failed = 202
Operator_not_available = 203
Missing_operator_ip_port = 204
Missing_dest_dbms_table = 205
Missing_par_in_file_name = 206
Wrong_tsd_id = 207
NETWORK_CONNECTION_FAILED = 208
BLOCKCHAIN_contract_error = 209
Missing_authorization = 210
No_network_peers = 211
Unauthorized_command = 212
Missing_root_user = 213
Missing_cluster_info = 214
Non_unique_policy = 215
ERR_attr_name = 216
Inconsistent_schema = 217
Wrong_license_key = 218
Error_str_format = 219
Operator_not_accessible = 220
Wrong_port_value = 221
Missing_cluster_policy = 222
Failed_to_assign_unused_port = 223
Non_supported_remote_call = 224
Unrecognized_data_type = 225
Topic_not_monitored = 226
gRPC_wrong_connect_info = 227
gRPC_process_failed = 228
ERR_in_script_cmd = 229
ERR_large_command = 230     # The command does not fit in a block
Prep_dir_not_defined = 231
Wrong_directory_name = 232
Wrong_rule_name = 233
Format_not_supported = 234
Config_Error = 235
Expired_license = 236
NOT_SUPPORTED = 237
ERR_msg_format = 238
Wrong_address = 239
ERR_json_search_key = 240
Profiler_lib_not_loaded = 241
Profiler_call_not_in_sequence = 242
Profiler_process_failure = 243
Err_grafana_payload = 244
Mapping_to_garafana_Error = 245
Failed_to_retrieve_network_reply = 246
Policy_id_not_match=247
Invalid_policy=248
Not_suppoerted_on_main_thread=249
Failed_to_parse_input_file = 250
Missing_source_file = 251
HTTP_failed_to_decode = 252


# note that message is at location of error value + 1 (exit is set at 0)
status_text = ["Terminating node processes",
               "Success",  # 0
               "Unrecognized Command",  # 1
               "Error Command Structure",  # 2
               "Error Process Failure",  # 3
               "Error IO",  # 4
               "Wrong DBMS Name or DBMS not connected",  # 5
               "Error in JSON Structure",  # 6
               "Error in Data Structure",  # 7
               "No reply",  # 8
               "Data size is larger than buffer",  # 9
               "DBMS not open",  # 10
               "Error missing brackets",  # 11
               "File does not exists",  # 12
               "Compare failure",  # 13
               "File copy failed",  # 14
               "Network Error",  # 15
               "SQL Failure",  # 16
               "Missing metadata info on local node",  # 17
               "SQL stmt not supported",  # 18
               "Blockchain not recognized",  # 19
               "IP and Port of destination servers are not properly provided",  # 20
               "IP and Port of source server are not properly provided or accessible",  # 21
               "Directory is in watch by a different process",  # 22
               "Wrong job ID",  # 23
               "DBMS error",  # 24
               "Query that inputs a target query returned with no data",  # 25
               "Time Out",  # 26
               "Failed to map JSON to SQL insert",  # 27
               "Wrong table name",  # 28
               "No metadata info",  # 29
               "Blockchain operation failed",  # 30
               "Failed to place new data in watch directory",  # 31
               "Failed to place new data in REST directory",  # 32
               "Missing configuration",  # 33
               "Python call failed",  # 34
               "Directory does not exists",  # 35
               "Failed 'write data stream' process",  # 36
               "Job processed is not active",  # 37
               "Failed to send an error message to node",  # 38
               "Operator node error",  # 39
               "Error in command parameters",  # 40
               "Unexpected reply returned from dbms",  # 41
               "DBMS is not open or Table not declared",  # 42
               "Error with IP and Port of connected DBMS",  # 43
               "File move process failed",  # 44
               "Failed to load table metadata from blockchain",  # 45
               "Failed to parse SQL statement",  # 46
               "File open failed",  # 47
               "File write failed",  # 48
               "DBMS connection error",  # 49
               "DBMS not connected",  # 50
               "REST call error",  # 51
               "Insert new row failed",  # 52
               "Update existing row failed",  # 53
               "Missing event script",  # 54
               "Argument mismatch in a script",  # 55
               "Error in script without 'on error' instruction",  # 56
               "The thread is not executing a script",  # 57
               "No local blockchain file",  # 58
               "Empty local blockchain file",  # 59
               "Failed to execute 'bring' from JSON",  # 60
               "System call terminated or failed",  # 61
               "Failed to extract dir/file/type from name",  # 62
               "Directory is not writeable",  # 63
               "Missing file name",  # 64
               "Compression Failed",  # 65
               "Table structure not available",  # 66
               "Failed to analyze JSON file",  # 67
               "Failed to pull blockchain from local dbms",  # 68
               "Wrong blockchain structure",  # 69
               "Duplicate blockchain object id",  # 70
               "Failed to delete file",  # 71
               "Failed to rename file",  # 72
               "Missing Operators for a Table",  # 73
               "Watch Directory not defined",  # 74
               "Inconsistent metadata declarations",  # 75
               "Query processing time above limit",  # 76
               "Query data volume above limit",  # 77
               "Wrong Path",  # 78
               "Missing partition information",  # 79
               "Drop partition failed",  # 80
               "Connection Error",  # 81
               "Missing partition declaration",  # 82
               "Missing DBMS name",  # 83
               "Input query not available",  # 84
               "Failed to retrieve instructions",  # 85
               "Duplicate JSON File",  # 86
               "Wrong policy structure",  # 87
               "Wrong username or password",  # 88
               "Email message failed",  # 89
               "Failed to read files from dir",  # 90
               "Missing password",  # 91
               "Failed to create directory",  # 92
               "Wrong password",  # 93
               "Duplicate signatures",  # 94
               "Public key is missing",  # 95
               "Internal Block Error",  # 96
               "Failed to authenticate message",  # 97
               "Failed to sign a message or wrong password",  # 98
               "Failed to import library",  # 99
               "Decompression failed",  # 100
               "DBMS error: delete row failed - wrong key",  # 101
               "A user with the provided name exists",  # 102
               "Failed to add new user",  # 103
               "Wrong DBMS type",  # 104
               "Scheduler not active",  # 105
               "Backup Failed",  # 106
               "Only subset of policies deleted",  # 107
               "Failed to allocate resources for task",  # 108
               "Blockchain table not on local DBMS",  # 109
               "Error in REST headers",  # 110
               "Drop DBMS failed",  # 111
               "Missing script file",           # 112
               "Needed Policy is not available",# 113
               "Incomplete Policy",
               "Wrong file type",               # 115
               "File time is not accessible",   # 116
               "Data from TSD table in almgm DBMS is not accessible",   # 117
               "Missing parent policy",         # 118
               "Error with file name structure",# 119
               "Empty result set",              # 120
               "A process with a TSD table failed", #121
               "MQTT connection failure",       # 122
               "MQTT publish failed",           # 123
               "Error with info provided to MQTT", # 124
               "Error with data provided by a subscribed message", # 125
               "MQTT: Failed to retrieve DBMS name from message", # 126
               "MQTT: Failed to retrieve Table name from message", # 127
               "MQTT: Wrong client ID",     # 128
               "File move to watch dir failed",     # 129
               "Table excluded from processing",    # 130
               "File from an unrecognized member of the cluster",     # 131
               "Local table not declared in blockchain", #132
               "Failed to retrieve hostname",           # 133
               "Error in SQL Select statement",         # 134
               "TCP server is not running",             # 135
               "Process is already active",             # 136
               "Failed to generate keys",               # 137
               "Uninitialized variable",                # 138
               "User authentication disabled",          # 139
               "File read failed",                      # 140
               "Unrecognized Command",                  # 141
               "Private key not available",             # 142
               "Encryption Failed",                     # 143
               "Decryption Failed",                     # 144
               "Authentication failed",                 # 145
               "Failed to extract message components",  # 146
               "No TSD tables",                         # 147
               "File move to archive failed",           # 148
               "None unique IP:Port in policy",          # 149
               "Blockchain synchronizer is not running", # 150
               "Policy not in local file",              # 151
               "No files satisfy the copy command",     # 152
               "Error in directory name",               # 153
               "Received non-subscribed message",       # 154
               "Failed to determine destination node",  # 155
               "Wrong HTTP method used",                # 156
               "MQTT Server Error",                     # 157
               "MQTT message not in JSON format",       # 158
               "Unrecognized MQTT topic",               # 159
               "Error in message received by broker",   # 160
               "Wrong output format",                   # 161
               "Task not in scheduler",                 # 162
               "Unknown user agent",                    # 163
               "No monitored info",                     # 164
               "Failed query process",                  # 165
               "Missing leading results",               # 166
               "Missing data ingest mapping",           # 167
               "Job terminated by caller",              # 168
               "Command not executed",                  # 169
               "Missing ID in policy",                  # 170
               "Operator not allowed to declare table", # 171
               "Files not available in directory",      # 172
               "Timezone error",                        # 173
               "No cluster assigned to operator",       # 174
               "Wrong value assigned to a key",         # 175
               "Error in test file structure",          # 176
               "Failed Kafka process",                  # 177
               "SQL not executed",                      # 178
               "Connected to different DBMS",           # 179
               "Non unique policy values",              # 180
               "Failed to parse if condition",          # 181
               "Error timeout value",                   # 182
               "Missing blob file name",                # 183
               "Missing key in dictionary",             # 184
               "Failed blob insert",                    # 185
               "Failed blob retrieve",                  # 186
               "Failed to list files",                  # 187
               "Data type not supported",               # 188
               "File not available in storage",         # 189
               "Command not supported by DBMS Engine",  # 190
               "Blobs Directory is not defined",        # 191
               "Operator processes not enabled",        # 192
               "Wrong header structure",                # 193
               "Casting Failure",                       # 194
               "Non supported casting",                 # 195
               "Failed to encode data",                 # 196
               "Duplicate Condition",                   # 197
               "File already exists",                   # 198
               "Encoding failed",                       # 199
               "Decoding Failed",                       # 200
               "Commit Failed",                         # 201
               "Rollback Failed",                       # 202
               "Operator not available",                # 203
               "Operator is missing IP:Port",           # 204
               "Missing destination DBMS or Table ",    # 205
               "Missing partition in file name",        # 206
               "Wrong TSD ID",                          # 207
               "Network connection to peer refused",    # 208
               "Blockchain contract error",             # 209
               "Missing policy authorization",          # 210
               "Metadata without network peers",        # 211
               "Unauthorized command",                  # 212
               "Missing_root_user",                     # 213
               "Cluster info not available",            # 214
               "Non unique policy",                     # 215
               "Wrong attribute name",                  # 216
               "Inconsistent Schema",                   # 217
               "Wrong license key",                     # 218
               "Error str format",                      # 219
               "Operator not accessible",               # 220
               "Wrong port value",                      # 221
               "Missing cluster policy",                # 222
               "Failed to assign unused port",          # 223
               "Non supported remote call",             # 224
               "Unrecognized data type",                # 225
               "Topic not monitored",                   # 226
               "gRPC wrong connection info",            # 227
               "gRPC process failed",                   # 228
               "Error in script command",               # 229
               "Error large command",                   # 230
               "Prep dir not defined",                  # 231
               "Wrong directory name",                  # 232
               "Wrong rule name",                       # 233
               "Format not supported",                  # 234
               "Configuration Error",                   # 235
               "Expired AnyLog license key",            # 236
               "Not Supported in this Version",         # 237
               "Error in message format",               # 238
               "Wrong Address",                         # 239
               "Error in JSON search key",              # 240
               "Profiler lib not loaded",               # 241
               "Profiler call not in sequence",          # 242
               "Profiler process failure",              # 243
               "Error in Grafana Payload info",         # 244
               "Error Mapping Data Source to Grafana",  # 245
               "Failed to retrieve reply from network",  # 246
               "Provided policy id does not match the policy", # 247
               "Policy is invalid",                     # 248
               "Command not suppoerted on main thread", # 249
               "Failed to parse input file",            # 250
               "Missing_source_file",                   # 251
               "HTTP Request: failed to decode message body",  # 252
               ]


WARNING_no_files_in_dir = 1
WARNING_empty_data_set = 2

# note that message is at location of error value + 1 (exit is set at 0)
warning_text = ["NO warning",  # 0
                "No files in dir",  # 1
                "Empty data set"]  # 2

LOCATION_ALL = 0
LOCATION_TCP = 1
LOCATION_REST = 2
LOCATION_SCRIPTS = 3
LOCATION_SCHEDULER = 4
LOCATION_SYNC = 5
LOCATION_OPERATOR = 6
LOCATION_PUBLISHER = 7
LOCATION_DISTRIBUTOR = 8
LOCATION_CONSUMER = 9
LOCATION_STREAMER = 10
LOCATION_BROKER = 11
LOCATION_KAFKA = 12
LOCATION_ARCHIVER = 13

global_processes = {
    "all": LOCATION_ALL,
    "tcp": LOCATION_TCP,
    "rest": LOCATION_REST,
    "scripts": LOCATION_SCRIPTS,
    "scheduler": LOCATION_SCHEDULER,
    "synchronizer": LOCATION_SYNC,
    "operator": LOCATION_OPERATOR,
    "publisher": LOCATION_PUBLISHER,
    "distributor": LOCATION_DISTRIBUTOR,
    "consumer": LOCATION_CONSUMER,
    "streamer": LOCATION_STREAMER,
    "broker": LOCATION_BROKER,
    "kafka": LOCATION_KAFKA,
    "archiver": LOCATION_KAFKA,
}
global_status = [SUCCESS] * len(global_processes)
global_signal = [SUCCESS] * len(global_processes)

process_sleep_event = {
    "synchronizer" : threading.Event(),
    "consumer" : threading.Event(),
}

string_codes_ = {
    "ignore entry" : IGNORE_ENTRY     # Used in streaming_conditions() to remove an entry from the list
}
# =======================================================================================================================
# Return number based on user provided string
# =======================================================================================================================
def get_return_code(some_string):
    '''
    Translate a string to a returned value
    '''
    global string_codes_
    if some_string in string_codes_:
        ret_val = string_codes_[some_string]
    else:
        ret_val = ERR_unrecognized_command
    return ret_val

# =======================================================================================================================
# Return text. Note that the message is located at status + 1 as EXIT has the value -1
# =======================================================================================================================
def get_status_text(error_code):
    if error_code >= NON_ERROR_RET_VALUE:
        if (error_code - NON_ERROR_RET_VALUE) < len(control_text):
            text = control_text[error_code - NON_ERROR_RET_VALUE]
        else:
            text = "Error #%u not recognized" % error_code
    else:
        if (error_code + 1) < len(status_text):
            text = status_text[error_code + 1]
        else:
            text = "Error #%u not recognized" % error_code
    return text

# =======================================================================================================================
# Replace a contro; returned code (like GOTO) with text
# =======================================================================================================================
def get_control_text(return_code):
    return control_text[return_code - NON_ERROR_RET_VALUE]

# =======================================================================================================================
# This places the thread on sleep that can be signaled
# =======================================================================================================================
def sleep_test_signal(sleep_time, process_name):
    global process_sleep_event
    sleep_event = process_sleep_event[process_name]
    flag = sleep_event.wait(sleep_time)        # sleep for the specified time or until signaled
    if flag:
        # Event has set to true()
        sleep_event.clear()

# =======================================================================================================================
# Signal process
# =======================================================================================================================
def set_signal(process_name):
    global global_signal
    global global_processes
    global process_sleep_event

    if process_name not in global_processes.keys():
        return False  # not a valid process name

    process_id = global_processes[process_name]

    global_signal[process_id] = True

    if process_name in process_sleep_event:
        # the thread can be signaled to exit sleep
        sleep_event = process_sleep_event[process_name]
        sleep_event.set()

    return True
# =======================================================================================================================
# Signal process
# =======================================================================================================================
def is_signaled(process_name):

    global global_signal
    global global_processes

    if process_name not in global_processes.keys():
        return False  # not a valid process name

    process_id = global_processes[process_name]

    if global_signal[process_id]:
        global_signal[process_id] = False
        ret_val = True
    else:
        ret_val = False

    return ret_val

# =======================================================================================================================
# User is terminating the process
# =======================================================================================================================
def set_exit(process_name):
    global global_status
    global global_processes

    if process_name not in global_processes.keys():
        return False  # not a valid process name

    process_id = global_processes[process_name]

    global_status[process_id] = EXIT

    if process_name in process_sleep_event:
        # the thread can be signaled to exit sleep
        sleep_event = process_sleep_event[process_name]
        sleep_event.set()

    return True

# =======================================================================================================================
# Reset the exit flag
# =======================================================================================================================
def reset_exit(process_name):
    global global_status
    global global_processes

    process_id = global_processes[process_name]

    global_status[process_id] = SUCCESS

# =======================================================================================================================
# Test termination
# =======================================================================================================================
def is_exit(process_name:str, reset_flag:bool = True):
    global global_status
    global global_processes

    if global_status[0] == EXIT:
        ret_val = True  # global process - exit all - all threads to exit
    else:
        if not process_name:
            ret_val = False     # Only exit node is tested
        else:
            # Stop specific process
            process_id = global_processes[process_name]
            if global_status[process_id] == EXIT:
                if process_name == "scripts":
                    # test that all scripts terminated
                    if not len(member_cmd.running_scripts):
                        # no more running scripts
                        global_status[process_id] = SUCCESS  # Reset flag
                if process_name == "scheduler":
                    if not task_scheduler.get_schedulers():
                        global_status[process_id] = SUCCESS  # All schedulers terminated
                else:
                    if reset_flag:
                        global_status[process_id] = SUCCESS  # Reset flag
                ret_val = True
            else:
                ret_val = False  # Exit flag was not raised

    return ret_val


class ProcessStat:

    def __init__(self):
        self.error_dest = "None"
        self.job_handle = job_handle.JobHandle(-1)  # job_handle maintains instructions and info relating dest of output
        self.if_result = False  # keeps the result of the last if stmt
        self.job_info = JobInfo()
        self.select_parsed = al_parser.SelectParsed()  # An object to parse SQL queries
        self.io_buff = None  # There are cases where the IO buff is per thread. I.E Streaming thread
        self.mutex_type = "R-"
        self.reset()

    # =======================================================================================================================
    # Get the per thread IO-Buff
    # =======================================================================================================================
    def get_io_buff(self):
        if self.io_buff == None:
            buff_size = int(params.get_param("io_buff_size"))
            self.io_buff = bytearray(buff_size)
        return self.io_buff

    # =======================================================================================================================
    # Provides an object with the query info
    # =======================================================================================================================
    def get_select_parsed(self):
        return self.select_parsed

    # =======================================================================================================================
    # Test if select_parsed is with non issued leading queries
    # =======================================================================================================================
    def is_with_non_issued_leading_queries(self):
        return self.select_parsed.is_with_non_issued_leading_queries()

    # =======================================================================================================================
    # TInitialize leading query process and return the first leading query
    # =======================================================================================================================
    def init_leading_query(self):
        self.select_parsed.init_leading_query()

    # =======================================================================================================================
    # Return the next non-issued leading query
    # =======================================================================================================================
    def get_next_leading_query(self):
        return self.select_parsed.get_next_leading_query()

    # =======================================================================================================================
    # Provide the Job Info
    # =======================================================================================================================
    def get_job_info(self):
        return self.job_info

    # =======================================================================================================================
    # Save the goto location
    # =======================================================================================================================
    def set_goto_name(self, name_location):
        self.goto_name = name_location

    # =======================================================================================================================
    # Return the goto location
    # =======================================================================================================================
    def get_goto_name(self):
        return self.goto_name

    # =======================================================================================================================
    # Flag the type of mutex used
    # =======================================================================================================================
    def flag_mutex(self, mutex_type):

        if mutex_type[1] == self.mutex_type[1]:
            self.add_error(f"Error in thread mutex order: New {mutex_type} over existing {self.mutex_type}")
        elif mutex_type[1] == '-' and mutex_type[0] != self.mutex_type[0]:
            self.add_error(f"Error in thread mutex release: mutex {mutex_type} releasing {self.mutex_type}")

        self.mutex_type = mutex_type

    # =======================================================================================================================
    # Test if mutexed
    # =======================================================================================================================
    def is_mutexed(self):
        return False if  self.mutex_type[1] == '-' else True

    # =======================================================================================================================
    # Set error destination (like stdout of a file)
    # =======================================================================================================================
    def set_error_dest(self, destination):
        self.error_dest = destination  # stdout or file name

    # =======================================================================================================================
    # Get error destination
    # =======================================================================================================================
    def get_error_dest(self):
        return self.error_dest  # stdout or file name

    # =======================================================================================================================
    # Save last error
    # =======================================================================================================================
    def keep_error(self, message: str):
        self.last_error = message

    # =======================================================================================================================
    # Get last error
    # =======================================================================================================================
    def get_saved_error(self):
        return self.last_error

    # =======================================================================================================================
    # Add to error log
    # Include stack trace if with_traceback_ is enabled by "set traceback on"
    # Print the stack trace, or = if the command "Set traceback on" specified text, test for the text in the error message
    # =======================================================================================================================
    def add_error(self, message: str):
        process_log.add("Error", message)
        if with_traceback_:
            if not traceback_text_ or message.find(traceback_text_) != -1:
                get_traceback(message)
    # =======================================================================================================================
    # Add to error log and keep as last message
    # =======================================================================================================================
    def add_keep_error(self, message):
        self.last_error = message
        self.add_error(message)

    # =======================================================================================================================
    # Add to event to log + output error if trace is not 0
    # =======================================================================================================================
    def add_error_trace(self, trace, message: str):
        process_log.add("Error", message)
        if trace:
            utils_print.output("Error:\t" + message, True)

    # =======================================================================================================================
    # Add to event to log
    # =======================================================================================================================
    def add_warning(self, message: str):
        process_log.add("Warning", message)

    # =======================================================================================================================
    # Add a message to event log
    # =======================================================================================================================
    def add_message(self, message: str):
        process_log.add("Message", message)

    # =======================================================================================================================
    # Add a message to event log - if trace is on, print message
    # =======================================================================================================================
    def add_trace(self, trace, message: str):
        process_log.add("Message", message)
        if trace:
            utils_print.output("Message:\t" + message, True)

    # =======================================================================================================================
    # Add a message to event log + print - only if trace is on
    # =======================================================================================================================
    def add_if_trace(self, trace, message: str):
        if trace:
            process_log.add("Message", message)

    # =======================================================================================================================
    # Set error value which was printed to the console
    # =======================================================================================================================
    def set_considerd_err_value(self, value: int):
        self.err_val = value

    # =======================================================================================================================
    # Get error value that was printed or returned to the app
    # =======================================================================================================================
    def get_considered_err_value(self):
        return self.err_val

    # =======================================================================================================================
    # Error Value is reset to allow prints of the error message inside nested calls to process_cmd
    # =======================================================================================================================
    def reset_considerd_err_val(self):
        self.err_val = 0

    # =======================================================================================================================
    # Set warning value
    # =======================================================================================================================
    def set_warning_value(self, value: int):
        self.warning_val = value

    # =======================================================================================================================
    # Set warning value
    # =======================================================================================================================
    def reset_warning_value(self):
        self.warning_val = 0

    # =======================================================================================================================
    # Get warning value
    # =======================================================================================================================
    def get_warning_value(self):
        return self.warning_val

    # =======================================================================================================================
    # Return text for the warning value.
    # =======================================================================================================================
    def get_warning_msg(self):
        return warning_text[self.warning_val]

    # =======================================================================================================================
    # Return text for error message
    # =======================================================================================================================
    def get_err_msg(self):
        return status_text[self.err_val + 1]

    # =======================================================================================================================
    # Move to the next command to execute in debug interactive mode
    # =======================================================================================================================
    def set_next_command(self, command: str):
        self.next_command = command
        self.with_next_command = True

    # =======================================================================================================================
    # Get the next command to execute
    # =======================================================================================================================
    def get_next_comand(self):
        return self.next_command

    # =======================================================================================================================
    # Test next command
    # =======================================================================================================================
    def is_with_next_comand(self):
        return self.with_next_command

    # =======================================================================================================================
    # Get the job handle that is on the current status object
    # =======================================================================================================================
    def get_job_handle(self):
        return self.job_handle

    # =======================================================================================================================
    # Get the active job handle that is used with this status object
    # =======================================================================================================================
    def get_active_job_handle(self):
        if self.job_id == -1:
            return self.job_handle
        else:
            return self.ptr_job_handle

    # =======================================================================================================================
    # Test the output socket of a REST server
    # =======================================================================================================================
    def test_output_socket(self, source):
        return self.job_handle.test_output_socket(source)

    # =======================================================================================================================
    # Set the job ID and a pointer to the job handle of a job instance.
    # - when the value is set (other than -1), the info of the job handle needs to be taken from the job handle
    # of the job instance
    # =======================================================================================================================
    def set_active_job_handle(self, j_id: int, j_handle: job_handle):
        self.job_id = j_id
        self.ptr_job_handle = j_handle

    # =======================================================================================================================
    # Get the job ID
    # =======================================================================================================================
    def get_job_id(self):
        return self.job_id

    # =======================================================================================================================
    # Reset job ID
    # =======================================================================================================================
    def reset_job_id(self):
        self.job_id = JOB_INSTANCE_NOT_USED

    # =======================================================================================================================
    # Flag indicating if needed to keep the registration of this thread on the watch directory
    # With repeatable commands, the value is set to True such that the thread maintains ownership of the directory
    # =======================================================================================================================
    def unregister_watch_dir(self, unregister_watch: bool):
        self.unregister_watch = unregister_watch

    # =======================================================================================================================
    # Flag indicating if needed to keep the registration of this thread on the watch directory
    # =======================================================================================================================
    def is_unregister_watch_dir(self):
        return self.unregister_watch

    # =======================================================================================================================
    # A flag indicating that the user needs to re-run the query.
    # This flag is set when a user issues a query and there is no metadata info. The query is replaced with a
    # request to retrieve the metadata and the flag indicates that the query needs to be re-issued
    # =======================================================================================================================
    def set_rerun_query(self):
        self.rerun_query = True

    # =======================================================================================================================
    # Test if a flag indicates that a query needs to be issued again
    # =======================================================================================================================
    def is_rerun_query(self):
        return self.rerun_query

    # =======================================================================================================================
    # Set a unique Job ID - this value is provided when this process sends a message
    # =======================================================================================================================
    def set_unique_job_id(self, unique_id):
        self.unique_job_id = unique_id

    # =======================================================================================================================
    # Get the unique Job ID - this value is provided when this process sends a message
    # =======================================================================================================================
    def get_unique_job_id(self):
        return self.unique_job_id

    # =======================================================================================================================
    # Set the is of the job scheduled
    # =======================================================================================================================
    def set_job_id_scheduled(self, sched_id, task_id):
        self.sched_id = sched_id     # Scheduler ID
        self.task_id = task_id      # task ID

    # =======================================================================================================================
    # If the job is a scheduled job - get the id
    # =======================================================================================================================
    def get_scheduler_id(self):
        return self.sched_id
    # =======================================================================================================================
    # get the task ID
    # =======================================================================================================================
    def get_task_id(self):
        return self.task_id

    # =======================================================================================================================
    # Keep the result of the if stmt
    # =======================================================================================================================
    def set_if_result(self, result):
        self.if_result = result

    # =======================================================================================================================
    # Get the result of the last 'if' stmt
    # =======================================================================================================================
    def get_if_result(self):
        return self.if_result
    # =======================================================================================================================
    # Flag that the reply from the different nodes needs to be provided to the REST client
    # =======================================================================================================================
    def set_rest_wait(self):
        self.rest_wait = True
    # =======================================================================================================================
    # Return True is a REST caller is on wait for a reply from the destinations nodes
    # =======================================================================================================================
    def is_rest_wait(self):
        return self.rest_wait

    # =======================================================================================================================
    # Subset determines if a reply is allowed even if not all nodes returned a reply
    # =======================================================================================================================
    def set_subset(self, subset : bool):
        self.job_handle.set_subset(subset)

    # =======================================================================================================================
    # Subset determines if a reply is allowed even if not all nodes returned a reply
    # =======================================================================================================================
    def is_subset(self):
        return self.job_handle.is_subset()

    # =======================================================================================================================
    # Timeout determines max time in seconds provided for a reply
    # =======================================================================================================================
    def set_timeout(self, timeout: int):
        self.job_handle.set_timeout(timeout)

    # =======================================================================================================================
    # Timeout determines max time in seconds provided for a reply
    # =======================================================================================================================
    def get_timeout(self):
        return self.job_handle.get_timeout()

    # =======================================================================================================================
    # The case where authentication is set to True or a call from a node with a certificate
    # =======================================================================================================================
    def set_public_key(self, pub_key):
        self.validate_permissions = True
        self.public_key = pub_key    # The public key of the node or user that triggered the process

    # -------------------------------------------------------------
    # Return the public key
    # -------------------------------------------------------------
    def get_public_key(self):
        return self.public_key

    # =======================================================================================================================
    # Determine if the database and table used in the SQL are allowed
    # =======================================================================================================================
    def is_need_permission(self):
        '''
        True is returned if the permissioned policies needs to be validated
        '''
        return self.validate_permissions

    # =======================================================================================================================
    # Return the public key of the Node or process with CVertificate that triggered the process
    # =======================================================================================================================
    def get_pub_key(self):
        return self.public_key
    # =======================================================================================================================
    # Store file data provided by a curl command
    # =======================================================================================================================
    def set_file_data(self, file_data):
        self.file_data = file_data
    # =======================================================================================================================
    # Get file data provided by a curl command
    # =======================================================================================================================
    def get_file_data(self):
        return self.file_data
    # =======================================================================================================================
    # Reset - Every thread have a dedicated job handle, that may be switched during the processing of data -->
    # --> return original object on reset
    # =======================================================================================================================
    def reset(self, pub_key = None):
        self.err_val = 0
        self.warning_val = 0
        self.next_command = None
        self.with_next_command = False
        self.last_error = ""  # the last error message
        self.job_id = JOB_INSTANCE_NOT_USED  # if the value is not -1, it represents job id --> use the job handle of the job_instance
        self.job_handle.reset()
        self.ptr_job_handle = None  # a pointer to the job handle of a job instance
        self.goto_name = ""

        # when a thread is reading a file name from a directory, the thread owns the directory.
        # True means ubregister after the read.
        self.unregister_watch = True
        self.rerun_query = False
        self.unique_job_id = 0  # a unique value provided when a message is send
        self.sched_id = 0    # Scheduler ID
        self.task_id = 0      # task ID

        self.rest_wait = False     # Set to true is REST thread is placed on wait

        self.file_data = None      # File data provided using curl -X POST -H "command: file store where dest = !prep_dir/file2.txt" -F "file=@testdata.txt" http://10.0.0.78:7849

        if pub_key:
            self.validate_permissions = True
            self.public_key = pub_key    # The public key of the node or user that triggered the process
        else:
            self.validate_permissions = False
            self.public_key = None

# =======================================================================================================================
# get the stacktrace
# =======================================================================================================================
def get_traceback(message):
    stack = repr(traceback.extract_stack())
    stack = stack.replace('>', '\n')
    stack += '\n' + message
    utils_print.output_box(stack)
# =======================================================================================================================
# get the code location from the stack
# =======================================================================================================================
def stack_to_location(stack_trace):
    try:
        code_location = str(stack_trace.tb_next.tb_frame)
    except:
        code_location = None
    if code_location:
        utils_print.output_box(code_location)
# =======================================================================================================================
# get the trace stack of all threads
# Examples:
# get stack trace
# get stack trace main
# get stack trace main tcp
# =======================================================================================================================
def get_stack_traces(status, io_buff_in, cmd_words, trace):

    reply = ""
    if len(cmd_words) == 3:
        bring_all = True # get stack trace - get all
    else:
        bring_all = False   # Only the maned threads
        bring_names = cmd_words[3:]     # get thread names
    try:
        for thread in threading.enumerate():
            if not bring_all:
                thread_name = thread.name.lower()
                get_stack = False
                for entry in bring_names:
                    if entry in thread_name:
                        # Bring the stack of this thread
                        get_stack = True
                        break
                if not get_stack:
                    continue        # Ignore the stack of this thread

            reply += f"\r\nStack trace of Thread {thread.name}:"
            frame = sys._current_frames().get(thread.ident)
            reply += ''.join(traceback.format_stack(frame))
    except:
        reply = "Failed to retrieve stack"
    return [SUCCESS, reply]
