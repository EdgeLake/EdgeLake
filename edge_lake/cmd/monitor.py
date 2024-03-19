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

# ---------------------------------------------------------------------------------------------------
# Collect and Monitor info from nodes in the network
# A dedicated node can update a local structure that contains status information on nodes in the network.
# The structure is build as a dictionary whereas the root is a topic, the second tier is IP + name of the monitored node
# The third layer is the info provided as key value pairs:
# [topic] --> [ip.Name] --> [key value pairs]
# When data is updated, 2 key-values are generated automatically:
#   a) date/time,
#   time elapsed from previous message
# ---------------------------------------------------------------------------------------------------
# examples:
''' 
schedule name = disk_space and time = 15 seconds task disk_space = get disk percentage .
schedule name = node_status and time = 15 seconds task node_status = get status format = json
task remove where scheduler = 1 and name = monitor_node 
schedule name = monitor_node and time = 15 seconds task run client 10.0.0.78:7848 monitor nodes where info = !node_status

schedule name = get_operator_stat and time = 15 seconds task operator_stat = get operator stat format = json
task remove where scheduler = 1 and name = monitor_operators
schedule name = monitor_operator and time = 15 seconds task run client 10.0.0.78:7848 monitor operators where info = !operator_stat
'''

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.process_status as process_status
import edge_lake.tcpip.message_header as message_header
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.params as params
import edge_lake.tcpip.net_utils as net_utils

network_status_ = {}

# ---------------------------------------------------------------------------------------------------
# Get a monitor command in the following structure
# monitor topic where ip = node-ip and name = node-name and info = json-struct
# monitor operator where ip = 127.0.0.1 and name = operator and info = { "total events" : 1000, "events per second" : 10 }
#  run client (!ip 7848) monitor operator where ip = 127.0.0.1 and name = operator and info = { "Total events" : 1000, "Events per second" : 10 }
# ---------------------------------------------------------------------------------------------------
def monitor_info(status, io_buff_in, cmd_words, trace):

    global network_status_

    ret_val = process_status.SUCCESS
    err_msg = None

    if len(cmd_words) < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        message = True
        mem_view = memoryview(io_buff_in)
        ip, port = message_header.get_source_ip_port(mem_view)
        command = message_header.get_command(mem_view)
        command = command.replace('\t', ' ')

        words_array, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, command, 0, 0)
        if left_brackets != right_brakets or len(words_array) < 6:
            ret_val = process_status.ERR_wrong_json_structure
    else:
        message = False
        words_array = cmd_words
        ip = net_utils.get_ip()
        port = net_utils.get_port()

    if len(words_array) < 6:
        return process_status.ERR_command_struct

    if not ret_val:
        topic = words_array[1]


        #                          Must     Add      Is
        #                          exists   Counter  Unique

        keywords = {"ip":   ("str", False, False, True),
                    "name": ("str", False, False, True),
                    "info": ("str", False, True, True),
                    "setup": ("bool", False, True, True),      # Only If the call is from the command:  set monitored nodes
                    }


        ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 3 , 0, keywords, False)
        if ret_val:
            return ret_val
        if not counter:
            # Need to include info, or be a setup call from the "set monitored nodes" command
            status.add_error("Monitor topic command is missing monitored info")
            return process_status.ERR_command_struct

        json_info = interpreter.get_one_value_or_default(conditions, "info", "{}")
        node_name = interpreter.get_one_value(conditions, "name")   # The name of the node Monitored
        node_ip = interpreter.get_one_value(conditions, "ip")  # The IP of the node Monitored
        is_setup = interpreter.get_one_value_or_default(conditions, "setup", False)

        json_struct = utils_json.str_to_json(json_info)
        if not json_struct and not is_setup:
            err_msg = "Data provided to monitoring is not in JSON format: %s" % json_info
            ret_val = process_status.ERR_wrong_json_structure
        elif not node_ip:
            if message:
                node_ip = ip    # take from the message
            else:
                node_ip = params.get_value_if_available("!external_ip")
            if not node_ip:
                err_msg = "Sender node IP is not available"
                ret_val = process_status.Error_command_params

        if not ret_val:
            if is_setup:
                # The call is from the command:  set monitored nodes
                node_id = node_ip
            else:
                if node_name:
                    node_id = node_ip + '.' + node_name     # Add node name if provided
                else:
                    node_id = node_ip + ":" + str(port)

            if topic not in network_status_:
                network_status_[topic] = {}


            if node_id not in network_status_[topic] or is_setup:
                info_dict = {}
                network_status_[topic][node_id] = info_dict
            else:
                info_dict = network_status_[topic][node_id]

            set_elapsed_time(info_dict, True)     # Update the time diff since last update of the node

            # Add message info
            for key, value in json_struct.items():
                info_dict[key] = value

        if ret_val and message:
            if err_msg:
                err_cmd = "echo Monitoring message failed: %s" % err_msg
            else:
                err_cmd = "echo Monitoring message failed: %s" % process_status.get_status_text(ret_val)

            member_cmd.error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, err_cmd, "")


    return ret_val

# ---------------------------------------------------------------------------------------------------
# Set the time diff from previous update
# ---------------------------------------------------------------------------------------------------
def set_elapsed_time(info_dict, is_update_time):
    '''
    info_dict is a dictionary maintaining info on a particulate node
    is_update_time - True - the method is called by a message from the node, False - the method is called by "get monitored operators" call
    '''
    current_time = utils_columns.get_current_time_in_sec()

    if "Update time" in info_dict:
        # Elapsed time from last update
        update_time = info_dict["Update time"]
        previous_time = utils_columns.get_time_in_sec(update_time)  # previous update time
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(current_time - previous_time)
        info_dict["Elapsed time"] = "%02u:%02u:%02u (H:M:S)" % (
        hours_time, minutes_time, seconds_time)  # Time since previous update

    if is_update_time:
        info_dict["Update time"] = utils_columns.seconds_to_date(current_time)  # Add current date and time


# ---------------------------------------------------------------------------------------------------
# Get the info on the monitored topics
# Examples:
#  get monitored
#  get monitored operator

# ---------------------------------------------------------------------------------------------------
def get_info(status, io_buff_in, cmd_words, trace):

    global network_status_

    ret_val = process_status.SUCCESS

    if len(cmd_words) == 2:
        # return all monitored topics
        info_str = "["
        for index, key in enumerate(network_status_):
            if index:
                info_str += (",\"" + key +  "\"")
            else:
                info_str += ("\"" + key + "\"")
        info_str += "]"
    else:
        topic = cmd_words[2]
        if topic in network_status_:
            # The Copy is needed as a different thread may change the size during iteration
            info_topic = network_status_[topic].copy()     # Get the info on a specific monitored info
            for node_info in info_topic.values():
                set_elapsed_time(node_info, False)     # Update the time diff since last update of the node
            info_str = utils_json.to_string(info_topic)
        else:
            ret_val = process_status.Topic_not_monitored
            info_str = ""

    return [ret_val, info_str, "json"]

# ------------------------------------------
# Reset the monitored info of a particular topic
# Example: reset monitored operators
# ------------------------------------------
def reset(status, io_buff_in, cmd_words, trace):

    topic = cmd_words[2]
    if topic in network_status_:
        network_status_[topic] = {} # Reset all info on the given topic
        ret_val = process_status.SUCCESS
    else:
        status.add_error("Command: 'reset monitored %s' failed: Topic %s is not monitored")
        ret_val = process_status.Error_command_params
    return ret_val
