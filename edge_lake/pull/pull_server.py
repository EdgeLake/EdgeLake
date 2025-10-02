
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
import time

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data

import edge_lake.pull.win_event_log as win_event_log
import edge_lake.pull.docker_stat as docker_stat


from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.params import get_param


# Processes Deployed by config or user calls
deployed_ = {

}

dispatch_ = {
    #  process      Prep - called once                  Run continuously
    "eventlog": (win_event_log.prep_win_event_log, win_event_log.get_windows_events),
    "docker":   (docker_stat.prep_docker_events, docker_stat.get_docker_stat),
}

# ----------------------------------------------------------------------------------------
# Add a pull method dynamically
# ----------------------------------------------------------------------------------------
def add_pull_method(process_key, prep_method, get_method):
    global dispatch_
    dispatch_[process_key]= (prep_method, get_method)

# ----------------------------------------------------------------------------------------
# Make repeatable calls for data
# run scheduled pull where
# ----------------------------------------------------------------------------------------
def call_data(userdata, conditions, trace):
    global dispatch_

    status = process_status.ProcessStat()

    process_name = conditions["name"][0]

    pull_type = conditions["type"][0]       # syslog / danfoss
    source = conditions["source"][0] if "source" in conditions else None
    frequency = conditions["frequency"][0]
    continuous = conditions["continuous"][0] if "continuous" in conditions else False # If true, pulls continuously as long as data is available, ignoring frequency

    topic_info = conditions["topic"][0] if "topic" in conditions else None

    if not topic_info:
        dbms_name = conditions["dbms"][0]
        table_name = conditions["table"][0]
        topic_name = ""
    else:
        topic_name = topic_info["name"][0]
        dbms_name = ""
        table_name = ""

    prep_dir = get_param("prep_dir")
    watch_dir = get_param("watch_dir")
    err_dir = get_param("err_dir")

    stdout = conditions["stdout"][0] if "stdout" in conditions else False


    process_info = {
                "type" : pull_type,
                "source": source,
                "log_type" : "",
                "event_type": "",
                "frequency" : frequency,
                "continuous" : continuous,
                "topic" : topic_name,
                "start_time": time.time(),
                "end_time": 0,
                "status": "running",
                "pull_count": 0, # Number of time data was pulled
                "event_count": 0,  # Number of events processed
                "win_flag": 0,  # Determine the type of log events to consider
                "error" : ""
    }
    deployed_[process_name] = process_info

    dispatch_params = {
        # Info provided to the pull function
        "process_info" : process_info,
    }

    prep_params = {
        # Info provided to the prep function
    }

    prep_params["conditions"] = conditions
    prep_params["source"] = source
    prep_params["process_info"] = process_info


    # Prepare the method to execute
    ret_val, err_msg = dispatch_[pull_type][0](status, prep_params, dispatch_params)  # Calls the specific pull command


    if not ret_val:

        process_info["dbms"] = dbms_name
        process_info["table"] = table_name

        while True:

            ret_val, events = dispatch_[pull_type][1](status, dispatch_params) # Calls the specific pull command

            process_info["pull_count"] += 1  # Count data pull
            if ret_val:
                break

            if events:

                if stdout:
                    # print to stdout and exit

                    json_str = utils_json.to_string(events)
                    utils_print.print_row(json_str, False)
                    utils_print.output("\n\r", True)
                    break   # Just once

                process_info["event_count"] += len(events)  # Count events processed

                for json_row in events:
                    json_msg = utils_json.to_string(json_row)

                    if topic_name:
                        ret_val = mqtt_client.process_message(topic_name, userdata, json_msg)  # Transfer data to MQTT Client
                    else:
                        ret_val, hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, dbms_name, table_name, '0', '0', "json", json_msg)
                    if ret_val:
                        break

                if ret_val:
                    break

                if not continuous or not len(events):
                    # Sleep if continuous is false or continuous is true but no data was pulled
                    time.sleep(frequency)
                if process_info["status"] == "exit":
                    break           # Process stopped

    if ret_val:
        process_info["error"] = process_status.get_status_text(ret_val)

    process_info["status"] = "terminated"

    message_color = "red" if ret_val else "green"
    utils_print.output_box(f"Process '{process_name}' in 'run scheduled pull' terminated with status: {process_status.get_status_text(ret_val)}", message_color)

    if err_msg:
        utils_print.output_box(err_msg)

# ----------------------------------------------------------------------------------------
# return statistics
# Command: get scheduled pull
# ----------------------------------------------------------------------------------------
def get_pull_info(status, io_buff_in, cmd_words, trace):

    info_table = []
    for process_name, process_info in deployed_.items():

        one_process = [
            process_name,
            process_info["type"],
            process_info["source"],
            process_info["log_type"],
            process_info["event_type"],
            process_info["frequency"],
            process_info["continuous"],
            process_info["topic"],
            process_info["dbms"],
            process_info["table"],
            process_info["status"],
            process_info["pull_count"],
            process_info["event_count"],
            process_info["error"],
            ]
        start_time = process_info["start_time"]
        end_time = process_info["end_time"]
        if not end_time:
            # take time up to now
            run_time = time.time() - start_time
        else:
            run_time = end_time - start_time
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(int(run_time))
        elapsed_time = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)
        one_process.append(elapsed_time)
        info_table.append(one_process)

    title = ["Name", "Type", "Source", "Log Type", "Event Type", "Frequency", "Continuous", "Topic", "DBMS", "Table", "Status", "Pull Count", "Event Count", "Error", "Elapsed Time"]
    info_string = utils_print.output_nested_lists(info_table, "\r\nStatistics", title, True, "")

    return [process_status.SUCCESS, info_string]


# ------------------------------------------------------------------------------
# Return an info string on the pull state
# ------------------------------------------------------------------------------
def get_info(status):
    global deployed_

    active_counter = 0

    if len(deployed_):
        for entry in deployed_.values():
            if entry["status"] == "running":
                active_counter += 1        # Count active processes

    if active_counter:
        info_str = "%u pull processes active" % active_counter
    else:
        info_str = ""

    return info_str

def is_active(process_name = None):
    global deployed_

    ret_val = False
    if len(deployed_):
        if not process_name:
            # return True of any process is runing
            for entry in deployed_.values():
                if entry["status"] == "running":
                    ret_val = True
                    break
        else:
            # test specific process
            if process_name in deployed_ and deployed_[process_name]["status"] == "running":
                ret_val = True
    return ret_val

# ------------------------------------------------------------------------------
# Terminate a process ot all processes
# ------------------------------------------------------------------------------
def exit(process_name):

    ret_val = process_status.SUCCESS
    if process_name == "all":
        for info in deployed_.values():
            info["status"] = "exit"
    elif process_name in deployed_:
        deployed_[process_name]["status"] = "exit"
    else:
        ret_val = process_status.Wrong_process_name
    return ret_val

