"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import threading
import time

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter
import edge_lake.dbms.db_info as db_info
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.streaming_conditions as streaming_conditions
import edge_lake.generic.version as version
from edge_lake.json_to_sql.map_json_to_insert import buffered_json_to_sql
from edge_lake.generic.process_log import add_and_print


bufferd_data_ = {}  # A dictionary to maintain buffered data as a f(dbms + table + source + instruction + type)
streaming_mutex = threading.Lock()  # global mutex to sync threads creating states
write_threshold = {}  # A dictionary that maintains, for every table the threshold time and volume to add the data to the database
default_time_ = 60  # A default 60 seconds threshold
default_volume_ = 10000  # A default 10000 bytes threshold
write_immediate_ = False       # Update the dbms on every new data item - or buffer before the update
stat_by_table_ = {}
counter_rows_in_buff_ = 0  # counter for rows in buffer,
counter_rows_flushed_ = 0 # rows flushed


is_running = False      # set to True if STreamer is running

def is_active():
    return is_running

# =======================================================================================================================
# Provides initial info to the 'get processes' command
# =======================================================================================================================
def get_info(status):
    global  is_running
    if is_running:
        reply = "Default streaming thresholds are %u seconds and %s bytes" % (default_time_, (f"{default_volume_:,}"))
    else:
        reply = ""
    return reply

# =======================================================================================================================
class StreamInfo:
    def __init__(self):
        self.flag_new_buff = True               # Indicates that the buff is first time initialized
        self.data_buffer = ""                   # ptr to the data,
        self.buffered_counter = 0               # Currently in the buffer (for statistics 0 satisfying "ge streaming" command
        self.buffer_mutex = threading.Lock()    # Mutex on the specific buffer
        self.current_time = 0                   # Time of placing first data in the buffer
        self.max_time = 0
        self.max_volume = 0
        self.is_immediate = False               # True/False to indicate write immediately to the database
        self.counter_im_rows = 0                # Counter for the number of immediate rows updated using the buff
        self.tsd_row_id = 0                     # Row ID in TSD table
        self.tsd_file_time = 0                  # file Time when TSD table was updated

        # Info needed to the operator
        self.dbms_name = ""
        self.table_name = ""
        self.source = ""
        self.instructions = ""
        self.file_type = ""


# =======================================================================================================================
# Get the buffer containing the stream messages for the requested table
# Details are available here - https://anylog.atlassian.net/wiki/spaces/ND/pages/1465843713/Adding+Data+to+a+local+database
# =======================================================================================================================
def get_stream_info(dbms_name, table_name, source, instructions, file_type, trace_level):
    global streaming_mutex
    global bufferd_data_

    # Mutex the table instance to avoid conflict of multiple threads

    # This key allows to provide the info to the Operator process and update the TSD tables
    key = "%s.%s.%s.%s.%s" % (dbms_name, table_name, source, instructions, file_type)

    if key in bufferd_data_:
        # already declared
        stream_info = bufferd_data_[key]
    else:
        streaming_mutex.acquire()  # A global mutex

        if key in bufferd_data_.keys():
            stream_info = bufferd_data_[key]  # Maybe created by a different thread before the mutex
        else:
            stream_info = StreamInfo()
            bufferd_data_[key] = stream_info  # Initialize for the specific KEY

            stream_info.dbms_name = dbms_name
            stream_info.table_name = table_name
            stream_info.source = source
            stream_info.instructions = instructions
            stream_info.file_type = file_type

        streaming_mutex.release()


    return stream_info

# =======================================================================================================================
# Get TSD file name - if the buffer data was updated using immediate flag - get the info on the TSD table updated
# =======================================================================================================================
def get_tsd_file_name(stream_info):
    if stream_info.is_immediate:
        tsd_file_name = "@.%s.%s" % (stream_info.tsd_row_id, str(stream_info.tsd_file_time))  # @ is the member ID - designating write immediate + row ID + TSD date
    else:
        tsd_file_name = None
    return tsd_file_name

# =======================================================================================================================
# Depending on the mode - add data to file or to internal buffers
# =======================================================================================================================
def add_data(status, mode, row_counter, prep_dir, watch_dir, err_dir, dbms_name, table_name, source, instructions, file_type, msg_data):

    global stat_by_table_
    global counter_rows_in_buff_        # Rows in the buffer waiting for flush
    global counter_rows_flushed_        # Rows written with the last flush call

    # Update stats

    hash_value = '0'
    ret_val = process_status.SUCCESS

    trace_level = member_cmd.commands["run streamer"]['trace']

    if streaming_conditions.is_with_conditions(dbms_name, table_name):
        # May return a different or empty msg data because entries removed
        ret_val, msg_data = streaming_conditions.apply_conditions(status, dbms_name, table_name, msg_data)

    if not ret_val and msg_data:
        key = "%s.%s" % (dbms_name, table_name)
        if key in stat_by_table_:
            stat_list = stat_by_table_[key]
        else:
            stat_list = [0,0,0,0,0, None]
            stat_by_table_[key] = stat_list     # File_counter, file_rows, stream_counter, stream_rows, immediate_counter

        stat_list[5] = int(time.time())

        counter_rows_in_buff_ += row_counter    # The number of rows in the buffer

        if mode == "file":
            # The default - data is written to prep dir and then moved to watch
            stat_list[0] += 1               # File_counter
            stat_list[1] += row_counter     # File_rows

            hash_value = utils_data.get_string_hash('md5', msg_data, dbms_name + '.' + table_name)  # Get the Hash of the data that is written to a file
            ret_val = add_data_prep_to_watch_dir(status, prep_dir, watch_dir, err_dir, dbms_name, table_name,
                                            source, instructions, None, file_type, msg_data, hash_value, trace_level)

        else:

            # Update stats
            stat_list[2] += 1               # stream_counter
            stat_list[3] += row_counter     # stream_rows

            stream_info = get_stream_info(dbms_name, table_name, source, instructions, file_type, trace_level)  # Get the structure to buffer the stream messages

            is_threshold, buff_data  = preprocess_msg(status, stream_info, dbms_name, table_name, source, instructions, msg_data, row_counter, trace_level)  # Set params on the new buffer + Update TSD Table

            if is_threshold:
                # threshold kicked to add data


                hash_value, tsd_file_name = post_process_buff(status, stream_info, dbms_name, table_name, buff_data, trace_level)


                ret_val = add_data_prep_to_watch_dir(status, prep_dir, watch_dir, err_dir, dbms_name, table_name,
                                                source, instructions, tsd_file_name, file_type, buff_data, hash_value, trace_level)


                counter_rows_flushed_ = counter_rows_in_buff_
                counter_rows_in_buff_ = 0       # Restart count rows in buff

    return [ret_val, hash_value]       # In case of a file - the hash value is returned

# =======================================================================================================================
# Complete the buffer processing:
# Update TSD table
# Calculate the hash value
# =======================================================================================================================
def  post_process_buff(status, stream_info, dbms_name, table_name, data_buff, trace_level):

    dbms_table =  dbms_name + '.' + table_name
    hash_value = utils_data.get_string_hash('md5', data_buff, dbms_table)  # Get the Hash of the data that is written to a file
    # if is_immediate - add the TSD info to the file name
    tsd_file_name = get_tsd_file_name(stream_info)
    if tsd_file_name:
        # When tsd entry was added - the data was not available and the hash value was set to include + dbms + table + date
        existing_hash = dbms_table + '.' + str(stream_info.tsd_file_time)  # WIth write immediate - Update the TSD info table
        try:
            # Update to represent the hash value of the data and the number of rows updated in write_immediate
            ret_val = db_info.tsd_update_streaming(status, existing_hash, hash_value, stream_info.counter_im_rows)
        except:
            ret_val = False

        if trace_level:
            utils_print.output("[Streaming] [Update TSD] [Rows: %u] [ret_val: %s] [%s.%s]" % (stream_info.counter_im_rows, str(ret_val), dbms_name, table_name), True)


    stream_info.tsd_file_time = 0  # Reset the file time

    return [hash_value, tsd_file_name]

# =======================================================================================================================
# Update a new buffer with info + Update the TSD table + Write Rows

# Processing streaming data - maintain a buffer that accumulates data
# data is accumulated and transferred to processing using size and time thresholds
# Data is added in 2 modes:
# write_immediate is True - local database table is immediately updated
# write_immediate is False - buffer accumulates and written as a JSON file to the watch dir and processed by the Operator process.
# Details are available here: https://anylog.atlassian.net/wiki/spaces/ND/pages/1465843713/Adding+Data+to+a+local+database

# =======================================================================================================================
def preprocess_msg(status, stream_info, dbms_name, table_name, source, instructions, new_data, row_counter, trace_level):

    stream_info.buffer_mutex.acquire()  # Mutex the specific key (specific table)
    if not stream_info.data_buffer and not stream_info.tsd_file_time:  # Empty buffer and tsd was not updated
        max_time, max_volume, write_immediate = get_write_threshold(dbms_name, table_name)  # Get the thresholds to flush the buff

        if stream_info.flag_new_buff:
            write_immediate = False     # First call is always False to create the table and operator
            stream_info.flag_new_buff = False

        stream_info.current_time = int(time.time())  # Init time of first data in the buffer
        stream_info.max_time = max_time
        stream_info.max_volume = max_volume
        stream_info.is_immediate = write_immediate
        stream_info.counter_im_rows = 0                  # Counter for the number of rows inserted with write_immediate flag
        if trace_level:
            utils_print.output("[Streaming] [New buffer] [Max Time: %u] [Max Volume: %u] [Write Immediate: %s]" % (
            max_time, max_volume, str(write_immediate)), True)

        if write_immediate:
            # UPDATE THE TSD TABLE
            try:
                write_rows = update_tsd_table(status, stream_info, dbms_name, table_name, source, instructions, trace_level)
                if not write_rows:
                    stream_info.is_immediate = False  # Cancel write immediate
            except:
                write_rows = False
                stream_info.is_immediate = False      # Cancel write immediate
        else:
            write_rows = False     # No immediate write
    else:
        # Take info from the buffer (when buffer was initialized, config params were set n the buffer) + write_immediate may have changed
        write_rows = stream_info.is_immediate       # True/False for write_immediate
        max_time = stream_info.max_time
        max_volume = stream_info.max_volume

    if write_rows:
        # UPDATE THE LOCAL TABLES
        # if write_completed is set to False, it signals failure in write and can trigger flush of the buff
        try:
            write_completed = write_rows_to_table(status, stream_info, dbms_name, table_name, source, instructions, new_data, trace_level)
            if not write_completed:
                stream_info.is_immediate = False  # Cancel write immediate
        except:
            write_completed = False
            stream_info.is_immediate = False  # Cancel write immediate
    else:
        write_completed = True    # Will not trigger flush of the buffer (unless thresholds are met).

    init_time = stream_info.current_time

    if write_completed:
        # Add existing data to buffer

        if not stream_info.data_buffer:  # Empty buffer - reset values
            stream_info.data_buffer = new_data
        else:
            stream_info.data_buffer += ('\n' + new_data)
        data_volume = len(stream_info.data_buffer)
        if is_write_threshold(max_time, max_volume, init_time, data_volume):
            stream_info.buffered_counter = 0  # Statistics - number of rows in the buff
            buff_data = stream_info.data_buffer
            stream_info.data_buffer = ""
            ret_val = True  # write the data
            # if is_immediate - add the TSD info to the file name
        else:
            stream_info.buffered_counter += row_counter     # Statistics - number of rows in the buff
            buff_data = ""
            ret_val = False  # Continue to accumulate
    else:
        # Write failed
        if stream_info.data_buffer:
            # WIth old data on the buffer
            buff_data = stream_info.data_buffer      # write only the older data
            stream_info.data_buffer = new_data       # Place the new data (that failed with write immediate) on the buffer
            stream_info.buffered_counter = row_counter # Statistics - number of rows in the buff
            ret_val = True  # write the OLD data
            # if is_immediate - add the TSD info to the file name
        else:
            # No older data
            data_volume = len(new_data)
            if is_write_threshold(max_time, max_volume, init_time, data_volume):
                stream_info.buffered_counter = 0  # Statistics - number of rows in the buff
                buff_data = new_data
                ret_val = True  # write the data
            else:
                stream_info.buffered_counter = row_counter  # Statistics - number of rows in the buff
                stream_info.data_buffer = new_data
                buff_data = ""
                ret_val = False  # Continue to accumulate

    stream_info.buffer_mutex.release()

    return [ret_val, buff_data ]       # return the data to write to file and the tsd name

# =======================================================================================================================
# Update the Local tables with the data
# =======================================================================================================================
def write_rows_to_table(status, stream_info, dbms_name, table_name, source, instructions, new_data, trace_level):

    row_id = stream_info.tsd_row_id  # The row ID in the TSD_INFO table
    data_list = new_data.split('\n')
    json_data = []
    last_entry = len(data_list)
    if last_entry > 1 and data_list[-1] == "":
        # Last entry is empty
        last_entry = -1     # Ignore the last empty lone

    for entry in data_list[:last_entry]:
        dict_entry = utils_json.str_to_json(entry)
        if dict_entry:
            json_data.append(dict_entry)

    sql_inserts, sql_counter = buffered_json_to_sql(status, dbms_name, table_name, source, instructions, row_id,
                                                    json_data)
    if not sql_counter:
        # Nothing was added - could be that partition is not defined - change flag to not immediate
        # Row remains in TSD info with counter as 0
        if trace_level:
            utils_print.output("[Streaming] [write_immediate] [Insert Failed] [%s.%s]" % (dbms_name, table_name), True)
        ret_val = False
    else:
        if trace_level:
            utils_print.output("[Streaming] [write_immediate] [Rows: %u] [%s.%s]" % (sql_counter, dbms_name, table_name), True)

        stream_info.counter_im_rows += sql_counter      # The number of rows updated
        stat_by_table_[dbms_name + '.' + table_name][4] += sql_counter  # Update statistics on the number of rows updated using write_immediate flag
        ret_val = True

    return ret_val
# =======================================================================================================================
# Update the TSD table with the info on the new buffer
# =======================================================================================================================
def update_tsd_table(status, stream_info, dbms_name, table_name, source, instructions, trace_level):

    row_id, file_time = db_info.insert_to_tsd_info(status, dbms_name, table_name, source, None, instructions, None,
                                                   None)
    if row_id == '0':
        # TSD not updated or not active
        stream_info.is_immediate = False  # Disable write_immediate
        if trace_level:
            utils_print.output(
                "[Streaming] [New buff] [write_immediate: True] [TSD update failed] [%s.%s]" % (dbms_name, table_name),
                True)
        ret_val = False
        stream_info.tsd_file_time = 0
    else:
        stream_info.tsd_row_id = row_id
        stream_info.tsd_file_time = file_time
        if trace_level:
            utils_print.output("[Streaming] [New buff] [write_immediate: True] [TSD Row: %s] [%s.%s]" % (
            row_id, dbms_name, table_name), True)

        ret_val = True

    return ret_val

# =======================================================================================================================
# Add data to watch dir by first writing to PREP DIR and then Moving the data to the WATCH DIR
# =======================================================================================================================
def add_data_prep_to_watch_dir(status, prep_dir, watch_dir, err_dir, dbms_name, table_name, source, instructions, tsd_file_name, file_type,
                          user_data, hash_value, trace_level):

    if version.prep_aggregations(dbms_name, table_name):  # Sets the monitored struct - or returns false if not monitored
        # Update the aggregations object for this table
        json_data = utils_json.str_to_json("[" + user_data.replace("\n",',') + ']')
        if json_data:
            ret_val, updated_data = version.process_agg_events(status, dbms_name, table_name, None, json_data)
            if ret_val ==  process_status.UPDATE_BOUNDS:
                # restructure as a string
                new_data = utils_json.to_string(updated_data)[1:-1].replace("},", "}\n")
            else:
                new_data = user_data
    else:
        new_data = user_data

    if tsd_file_name:
        # With write immediate - needs to add the TSD info
        file_name = "%s.%s.%s.%s.%s.%s" % (dbms_name, table_name, source, hash_value, instructions, tsd_file_name)
    else:
        file_name = "%s.%s.%s.%s.%s" % (dbms_name, table_name, source, hash_value, instructions)

    ret_val, prep_file = utils_io.make_path_file_name(status, prep_dir, file_name, file_type)
    if not ret_val:

        # Write the file to prep dir
        if not utils_io.write_str_to_file(status, new_data, prep_file):
            # Write failed - Update file name to include the error and write to error
            ret_val = process_status.ERR_write_stream
            extended_name = ".err_%u" % ret_val
            ret_val, prep_file = utils_io.make_path_file_name(status, err_dir, file_name + extended_name, file_type)
            utils_io.write_str_to_file(status, new_data, prep_file)
        else:
            # Move the file to the watch dir
            if not  utils_io.move_file(prep_file, watch_dir):
                # Move failed - make name unique abd move to the error dir
                ret_val = process_status.File_move_to_watch_failed
                extended_name = "err_%u" % ret_val
                utils_io.file_to_dir(status, err_dir, prep_file, extended_name, True)

    return ret_val

# =======================================================================================================================
# Write the data to the watch dir
# =======================================================================================================================
def write_data_to_watch_dir(status, watch_dir, dbms_name, table_name, source, instructions, tsd_file_name, file_type, new_data, hash_value, trace_level):

    if tsd_file_name:
        # With write immediate - needs to add the TSD info
        file_name = "%s.%s.%s.%s.%s.%s" % (dbms_name, table_name, source, hash_value, instructions, tsd_file_name)
    else:
        file_name = "%s.%s.%s.%s.%s" % (dbms_name, table_name, source, hash_value, instructions)
    ret_val, prep_file = utils_io.make_path_file_name(status, watch_dir, file_name, file_type)

    if not ret_val:

        if not utils_io.write_str_to_file(status, new_data, prep_file):
            ret_val = process_status.ERR_place_data_in_rest_dir

    return ret_val
# =======================================================================================================================
# Get the write thresholds from the configurations
# =======================================================================================================================
def get_write_threshold(dbms_name, table_name):
    global write_threshold
    global default_time_
    global default_volume_
    global write_immediate_

    key = dbms_name + '.' + table_name

    if key in write_threshold.keys():
        value = write_threshold[key]
        max_time = value[0]  # time threshold in seconds
        max_volume = value[1]  # volume threshold in bytes
        write_immediate = value[2] # is delay in updating the local database with the data
    elif dbms_name in write_threshold.keys():
        # a default value for all the tables in the database
        value = write_threshold[dbms_name]
        max_time = value[0]  # time threshold in seconds
        max_volume = value[1]  # volume threshold in bytes
        write_immediate = value[2]  # is delay in updating the local database with the data
    else:
        max_time = default_time_
        max_volume = default_volume_
        write_immediate = write_immediate_ # is delay in updating the local database with the data
    return [max_time, max_volume, write_immediate]

# =======================================================================================================================
# Test if the accumulated streaming data needs to be added to the database
# =======================================================================================================================
def is_write_threshold(max_time, max_volume, init_time, data_volume):
    '''
    max_time - the max time allowed to buffer (config param)
    max_volumr - the max volumr allowed to buffer (config param)
    init_time - the time when the buffer was initialized
    data_volume - contained in the buff
    '''

    if max_time and (int(time.time()) - init_time) >= max_time:
        ret_val = True  # Data needs to be processed by time threshold
    elif data_volume > max_volume:
        ret_val = True  # Data needs to be processed by volume threshold
    else:
        ret_val = False

    return ret_val

# =======================================================================================================================
# Operator code or Publisher code to manage the buffered data
# Operates scan through the buffered data and process the data that reached the time threshold
# =======================================================================================================================
def flush_buffered_data(status, watch_dir, prep_dir = None, err_dir = None):
    global bufferd_data_

    goto_sleep = True  # The returned value if no data was flushed

    trace_level = member_cmd.commands["run streamer"]['trace']

    for key, stream_info in bufferd_data_.items():

        stream_info.buffer_mutex.acquire()  # Mutex the specific key (specific table)

        new_data = stream_info.data_buffer

        if new_data:

            dbms_name = stream_info.dbms_name
            table_name = stream_info.table_name

            init_time = stream_info.current_time
            max_time = stream_info.max_time
            max_volume = stream_info.max_volume

            if is_write_threshold(max_time, max_volume, init_time, 0):

                stream_info.data_buffer = ""
                stream_info.buffered_counter = 0  # Statistics - number of rows in the buff

                hash_value, tsd_file_name = post_process_buff(status, stream_info, dbms_name, table_name, new_data, trace_level)

                if prep_dir:
                    # write the file to a prep dir and move to watch dir
                    ret_val = add_data_prep_to_watch_dir(status, prep_dir, watch_dir, err_dir, dbms_name, table_name, stream_info.source,
                                          stream_info.instructions, tsd_file_name, stream_info.file_type, new_data, hash_value, trace_level)
                else:
                    ret_val = write_data_to_watch_dir(status, watch_dir, dbms_name, table_name, stream_info.source, stream_info.instructions, tsd_file_name, stream_info.file_type, new_data, hash_value, trace_level)
                goto_sleep = False      # File was written, thread should run through all the files without a single write prior to sleep


        stream_info.buffer_mutex.release()

    return goto_sleep

# =======================================================================================================================
# Set thresholds for different data types from the command line
# =======================================================================================================================
def set_thresholds(status, io_buff_in, cmd_words, trace, publisher_active):
    global write_threshold
    global default_time_
    global default_volume_
    global write_immediate_     # If False - No delay when dbms is updated

    #                          Must     Add      Is
    #                          exists   Counter  Unique
    keywords = {
        "dbms": ("str", False, False, True),
        "table": ("str", False, False, True),
        "time": ("int.time", False, True, True),
        "volume": ("int.storage", False, True, True),
        "write_immediate" : ("bool", False, True, True),         # If delay is False (default), the data is immidiately written to the tables
    }

    if len(cmd_words) < 7 or cmd_words[3] != "where":
        return process_status.ERR_command_struct

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not counter:
        status.add_error("Missing Time threshold or Volume threshold in the 'set buffer threshold' command")
        return process_status.Missing_configuration

    dbms_name = interpreter.get_one_value(conditions, "dbms_name")
    table_name = interpreter.get_one_value(conditions, "table_name")

    if table_name and not dbms_name:
        status.add_error("Missing database name in 'set buffer threshold' command")
        return process_status.Missing_configuration

    threshold_time = interpreter.get_one_value_or_default(conditions, "time", 60)
    threshold_volume = interpreter.get_one_value_or_default(conditions, "volume", 10000)
    w_immediate = interpreter.get_one_value_or_default(conditions, "write_immediate", False)

    if w_immediate and publisher_active:
        status.add_error("Publisher service can't be enabled if 'run_immediate' flag is set to True")
        return process_status.Config_Error

    if dbms_name:
        if not table_name:
            key = dbms_name
        else:
            key = dbms_name + '.' + table_name

        if not threshold_time and not threshold_volume:
            if key in write_threshold.keys():
                del write_threshold[key]
        else:
            write_threshold[key] = [threshold_time, threshold_volume, w_immediate]  # set the time and volume threshold
    else:
        default_time_ = threshold_time
        default_volume_ = threshold_volume
        write_immediate_ = w_immediate

    return process_status.SUCCESS

# =======================================================================================================================
# Return Streaming Info to user command: get streaming

# stat_list[0] - counter for PUT calls with mode = file
# stat_list[1] - counter for rows in PUT calls with mode = file
# stat_list[2] - counter calls with mode = streaming
# stat_list[3] - counter calls with mode = streaming
# stat_list[4] - counter for rows written to a table with write immediate flag

# Command 1: get streaming setup
# Command 2 get streaming


# =======================================================================================================================
def show_info(config_only, out_format):
    '''
    config_only - Only Configuration
    out_format - Output Format - Table or JSON
    '''
    global write_threshold  # A dictionary that maintains, for every table the threshold time and volume to add the data to the database
    global default_time_  # A default 60 seconds threshold
    global default_volume_  # A default 1000 bytes threshold
    global stat_by_table_
    global is_running
    global counter_rows_in_buff_        # Rows in the buffer waiting for flush
    global counter_rows_flushed_        # Rows written with the last flush call

    # Provide the default Thresholds

    if config_only:
        if is_running:
            streamer_stat = "Running"
        else:
            streamer_stat = "Not Running"

        if out_format == "json":
            info_json = {}
            info_json["status"] = streamer_stat
            info_json["Threshold_Time"] = default_time_
            info_json["Threshold_Volume"] = default_volume_
            info_json["Write_Immediate"] = write_immediate_
            info_json["Buffered_Rows"] = counter_rows_in_buff_
            info_json["Flushed_Rows"] = counter_rows_flushed_

            info_string =  utils_json.to_string((info_json))

        else:
            info_chart = []
            info_chart.append(("Threshold Time", default_time_, streamer_stat))
            info_chart.append(("Threshold Volume", format(default_volume_,","),""))
            info_chart.append(("Write Immediate", str(write_immediate_), ""))
            info_chart.append(("Buffered Rows", format(counter_rows_in_buff_, ","), ""))
            info_chart.append(("Flushed Rows", format(counter_rows_flushed_, ","), ""))
            info_string = utils_print.output_nested_lists(info_chart, "Flush Thresholds", ["Threshold", "Value", "Streamer"], True, "")

            # Provide the Thresholds per Table
            if len(write_threshold):
                info_string += utils_print.format_dictionary(write_threshold, True, False, False, ["Table", "Time / Volume"])

    else:
        # Print per table

        if out_format == "json":
            tables_list = []
            for dbms_table, stat_list in stat_by_table_.items():
                table_info = {}
                cached_rows, threshold_volume, volume_used, threshold_time, remaining_time = get_cached_info(dbms_table)

                table_info["table"] = dbms_table
                table_info["File Put"] = stat_list[0]
                table_info["File Rows"] = stat_list[1]
                table_info["Streaming Put"] = stat_list[2]
                table_info["Streaming Rows"] = stat_list[3]
                table_info["Streaming Cached"] = cached_rows # Get the number of rows that are still in buffer and not flushed to disk
                table_info["Immediate"] = stat_list[4]
                table_info["Threshold Volume"] = threshold_volume
                table_info["% Buffer Fill"] = volume_used
                table_info["Threshold Time"] = threshold_time
                table_info["Time Left"] = remaining_time

                elapsed_time = int(time.time()) - stat_list[5]  # Since last time data was added
                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(elapsed_time)
                table_info["Last Process"] = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)


                tables_list.append(table_info)

            info_string = utils_json.to_string((tables_list))

        else:
            # Provide data statistics -  File_counter, file_rows, stream_counter, stream_rows
            info_chart = []
            for dbms_table, stat_list in stat_by_table_.items():
                files_put = format(stat_list[0], ",")
                files_rows = format(stat_list[1], ",")
                stream_put = format(stat_list[2], ",")
                stream_rows = format(stat_list[3], ",")
                counter_immediate = format(stat_list[4], ",")   # Rows with count immediate



                elapsed_time = int(time.time()) - stat_list[5]  # Since last time data was added
                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(elapsed_time)
                last_process = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

                cached_rows, threshold_volume, volume_used, threshold_time, remaining_time = get_cached_info(dbms_table)
                cached_rows = format(cached_rows, ",")  # Get the number of rows that are still in buffer and not flushed to disk


                info_chart.append((dbms_table, files_put, files_rows, "", stream_put, stream_rows, cached_rows, counter_immediate, threshold_volume, volume_used, threshold_time, remaining_time, last_process))

            title = ["\nDBMS-Table", "Put\nfiles", "Put\nRows", " ", "Streaming\nCalls", "Streaming\nRows", "Cached\nRows", "Counter\nImmediate", "Threshold\nVolume(KB)", "Buffer\nFill(%)", "Threshold\nTime(sec)", "Time Left\n(Sec)", "Last Process\nHH:MM:SS"]
            info_string = utils_print.output_nested_lists(info_chart, "\r\nStatistics", title, True, "")


    return info_string

# =======================================================================================================================
# Get info per each table (of data that is not flushed)
# bufferd_data_ maintains info on data that is in cache and not yet flushed
# The key in bufferd_data_ includes the dbms + table + source + instruction + type - therefore:
# to determine the total cached it is needed to aggregate all instances with the same key prefix.
# =======================================================================================================================
def get_cached_info(dbms_table):
    '''
    Return:
        number of rows in the cache
        Threshold storage (KB)
        percentage of the storage used
        Threshold time (seconds)
        remaining time (seconds)

    '''
    global bufferd_data_  # data that is in cache and not yet flushed

    cached_rows = 0
    threshold_volume = 0
    used_volume = 0     # The length of the current buffer used
    threshold_time = 0
    current_time = 0
    for key, stream_info in bufferd_data_.items():
        if key.startswith(dbms_table):
            cached_rows += stream_info.buffered_counter
            threshold_volume += stream_info.max_volume
            used_volume += len(stream_info.data_buffer)
            if stream_info.max_time > threshold_time:
                threshold_time = stream_info.max_time
            if stream_info.current_time > current_time:
                current_time = stream_info.current_time     # Time when first batch to an existing buff is added

    if not threshold_volume:
        volume_used = 0
    else:
        volume_used = round(100 * used_volume / threshold_volume, 2)

    if cached_rows:
        remaining_time = threshold_time - (int(time.time()) - current_time)
    else:
        remaining_time = threshold_time

    return [cached_rows, int(threshold_volume / 1000), volume_used, threshold_time, remaining_time]

# =======================================================================================================================
# Test for time thresholfd and Write the buffers to disk
# =======================================================================================================================
def flush_buffers(dummy: str, conditions: dict):

    global is_running

    is_running = True       # indicates streamer is running

    status = process_status.ProcessStat()
    watch_dir = interpreter.get_one_value(conditions, "watch_dir")
    err_dir = interpreter.get_one_value(conditions, "err_dir")
    prep_dir = interpreter.get_one_value(conditions, "prep_dir")


    while True:

        if process_status.is_exit("streamer"):
            break

        is_sleep = flush_buffered_data(status, watch_dir, prep_dir, err_dir)

        if is_sleep:
            time.sleep(10)


    add_and_print("event", "Streamer process terminated")       # Update the event log

    is_running = False      # Streamer terminated


# =======================================================================================================================
# Return the write_immediate flag
# =======================================================================================================================
def is_write_immediate():
    return write_immediate_