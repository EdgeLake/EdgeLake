"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import socket
import platform
import os
import time
import sys

import threading

try:
    import psutil  # For windows need to install: 'pip3 install psutil' and place in packages/windows dir
except:
    errno, value = sys.exc_info()[:2]
    print(f"\n\n\rFailed to load 'psutil' : errno: {str(errno)}  value: ({value})\n\r\n")
    psutil_installed = False
else:
    psutil_installed = True

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.params as params
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.streaming_data as streaming_data
import edge_lake.blockchain.metadata as metadata
from edge_lake.generic.input_kbrd import get_enter_counter
import edge_lake.cmd.member_cmd as member_cmd
from edge_lake.members.aloperator import output_info, output_summary
from edge_lake.generic.node_info import get_node_name, test_import

continuous_status_ = False  # Set to true if continuous is running

get_pool_cmd = ["get", None, "pool"]

def get_operator(status, cmd_words):
    return output_info(status)

def get_operator_summary(status, cmd_words):
    return output_summary(status)

def get_streaming(status, cmd_words):
    return streaming_data.show_info("table")

def get_threads_status(status, cmd_words):
    reply = member_cmd.get_pool_status(status, None, cmd_words, None)
    return reply[1]



def show_cpu_usage(status, cmd_words):
    '''
    continuous cpu
    Overall CPU
    '''
    words_count = len(cmd_words)
    if words_count == 1:
        # Total CPU
        try:
            cpu_info = psutil.cpu_percent(percpu=True)
            memory_info = psutil.virtual_memory()  # physical memory usage
            memory = memory_info.used / 1048576
        except:
            cpu_per = 0
            memory = 0
        else:
            cpu_per = 0
            for idx, usage in enumerate(cpu_info):
                cpu_per += usage
            cpu_per = cpu_per / len(cpu_info)       # Average on all CPUs
        name = "Total"
    else:
        cpu_count = psutil.cpu_count()
        pid_list = []       # A list with the process IDs to monitor
        if cmd_words[1] == "anylog":
            pid_list.append(os.getpid())    # Get AnyLog process ID
            name = "AnyLog"
        else:
            key = cmd_words[1]
            counter = 0
            for proc in psutil.process_iter():
                pname = proc.name().lower()
                if pname.startswith(key):
                    pid_list.append(proc.pid)
                    counter += 1        # Count processes with that name
            if key[0] >= 'a' and key[0] <= 'z':
                name = key[0].upper() + key[1:]
            else:
                name = key
            name += f" ({counter})"

        try:
            cpu_per = 0
            memory = 0
            for pid in pid_list:
                proc = psutil.Process(pid)
                ret_val, process_info = retrieve_process_info(status, proc, cpu_count)
                if not ret_val:
                    memory +=  process_info[2]
                    cpu_per += process_info[3]
                else:
                    memory = 0
                    cpu_per = 0
        except:
            cpu_per = 0


    cpu_per = round(cpu_per,2)
    cpu_used = str(cpu_per)
    if cpu_used[-2] == '.':
        cpu_used += '0'  # make sure 2 decimals
    cpu_used = cpu_used.rjust(6)
    mem_mb = format(round(memory,2), ",")
    if mem_mb[-2] == '.':
        mem_mb += '0'  # make sure 2 decimals
    mem_mb = mem_mb.rjust(10)

    reply = (name.ljust(15) + f" CPU: {cpu_used}%  Memory: {mem_mb}MB").ljust(60) + utils_print.print_status(100, 50, int(cpu_per), True)
    return reply

def show_pid_cpu_usage(status, cmd_words):
    '''
    continuous "cpu anylog"
    Overall CPU
    '''
    try:
        cpu_count = psutil.cpu_count()
        pid = os.getpid()  # Get AnyLog process ID
        proc = psutil.Process(pid)
        ret_val, process_info = retrieve_process_info(status, proc, cpu_count)
        if not ret_val:
            cpu_per = process_info[3]
        else:
            cpu_per = 0
    except:
        cpu_per = 0

    reply = f"AnyLog CPU Usage {round(cpu_per,2)}%:".ljust(30) + utils_print.print_status(100, 50, int(cpu_per), True)
    return reply

def get_each_cpu_usage(status, cmd_words):
    reply = get_cpu_usage(status, None, cmd_words, 0)
    return reply[1]

commands_dict_  = {
    "getoperator" : get_operator,
    "getoperatorsummary" : get_operator_summary,
    "getstreaming" : get_streaming,
    "getxpool" : get_threads_status,
    "getoperatorpool" : get_threads_status,
    "getquerypool" : get_threads_status,
    "getrestpool" : get_threads_status,
    "gettcppool" : get_threads_status,
    "getmsgpool": get_threads_status,
    "cpu": show_cpu_usage,
    "getcpuusage": get_each_cpu_usage,

}

# ---------------------------------------------------------
# Alpine failed with psutil.getloadavg
# https://psutil.readthedocs.io/en/latest/
# ---------------------------------------------------------
def set_psutil_ptrs( name_ptr_dict ):

    try:
        function = psutil.cpu_percent
    except:
        function = None
    finally:
        name_ptr_dict["cpu_percent"] = function # Return a float representing the current system-wide CPU utilization as a percentage

    try:
        function = psutil.cpu_times
    except:
        function = None
    finally:
        name_ptr_dict["cpu_times"] = function # Return system CPU times as a named tuple. Every attribute represents the seconds the CPU has spent in the given mode.

    try:
        function = psutil.cpu_times_percent
    except:
        function = None
    finally:
        name_ptr_dict["cpu_times_percent"] = function #  utilization percentages for each specific CPU

    try:
        function = psutil.getloadavg
    except:
        function = None
    finally:
        name_ptr_dict["getloadavg"] = function # the average system load over the last 1, 5 and 15 minutes

    try:
        function = psutil.swap_memory
    except:
        function = None
    finally:
        name_ptr_dict["swap_memory"] = function # Return system swap memory statistics as a named tuple

    try:
        function = psutil.disk_io_counters
    except:
        function = None
    finally:
        name_ptr_dict["disk_io_counters"] = function # Return system-wide disk I/O statistics as a named tuple

    try:
        function = psutil.net_io_counters
    except:
        function = None
    finally:
        name_ptr_dict["net_io_counters"] = function # Return system-wide disk I/O statistics as a named tuple



if psutil_installed:

    monitored_calls = {}
    set_psutil_ptrs(monitored_calls)

# ---------------------------------------------------------
# Monitor current machine status
# Details on psutil - https://psutil.readthedocs.io/en/latest/
# Command: get node info cpu_percent
# Command: get node info cpu_percent into dbms = monitor and table = cpu_percent
# ---------------------------------------------------------
def get_node_info(status, io_buff_in, cmd_words, trace):


    reply = ""
    if not psutil_installed:
        ret_val =  test_import(status, "psutil")
    else:
        ret_val = process_status.ERR_process_failure

        if cmd_words[1] == '=':
            offset = 2
        else:
            offset = 0
        words_count = len(cmd_words) - offset

        func_name = cmd_words[ offset + 3 ]

        if func_name in monitored_calls and monitored_calls[func_name] != None:
            try:
                func_reply = monitored_calls[func_name]()
            except:
                reply = ""
                status.add_error("Failed to monitor node with psutil.%s" % func_name)

            else:
                if words_count == 4:
                    # Return value to the caller
                    ret_val = process_status.SUCCESS
                    reply = str(func_reply)
                elif words_count == 5:
                    # For example: get node info net_io_counters bytes_recv
                    info_name = cmd_words[offset + 4]
                    try:
                        value = getattr(func_reply, info_name)
                    except:
                        status.add_error("Object net_io_counters without attribute '%s'" % info_name)
                        ret_val = process_status.ERR_attr_name
                    else:
                        ret_val = process_status.SUCCESS
                        reply = str(value)

                elif words_count == 12 and \
                        utils_data.is_sub_array(cmd_words, offset + 4, ["into", "dbms", "="])\
                        and utils_data.is_sub_array(cmd_words, offset + 8, ["and", "table", "="]):
                    # Example: get node info cpu_percent into dbms = monitor and table = cpu_percent

                    reply = ""
                    data_row = None
                    dbms_name = params.get_value_if_available(cmd_words[7])
                    table_name = params.get_value_if_available(cmd_words[11])
                    prep_dir = params.get_value_if_available("!prep_dir")
                    watch_dir = params.get_value_if_available("!watch_dir")
                    err_dir = params.get_value_if_available("!err_dir")
                    if not prep_dir or not watch_dir or not err_dir:
                        if not prep_dir:
                            directory = "!prep_dir"
                        elif not watch_dir:
                            directory = "!watch_dir"
                        else:
                            directory = "!err_dir"
                        status.add_error("Missing directory definition: %s" % directory)
                    else:
                        # write to streamer

                        timestamp = utils_columns.get_current_utc_time()
                        if isinstance(func_reply, float):
                            data_row = "{\"timestamp\": \"%s\", \"value\" : %f}" % (timestamp, func_reply)

                        else:
                            # write multiple values Example: get node info disk_io_counters into dbms = monitor and table = disk_counters
                            data_row = "{\"timestamp\": \"%s\"" % timestamp
                            try:
                                fields_count = len(func_reply._fields)
                                for index in range (fields_count):
                                    field_name = func_reply._fields[index]
                                    field_value = func_reply[index]
                                    data_row += ", \"%s\": %s" % (field_name, field_value)
                            except:
                                status.add_error("Failed to retrieve statistics")
                                data_row = None
                            else:
                                data_row += "}"

                        if data_row:
                            ret_val, hash_value = streaming_data.add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir,
                                                              dbms_name, table_name, '0', '0', 'json', data_row)


    return [ret_val, reply]

# ---------------------------------------------------------
# Get memory info
# ---------------------------------------------------------
def get_memory_info(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) != 3 or cmd_words[2] != "info":
        reply = ""
        ret_val = process_status.ERR_command_struct
    else:
        if not psutil_installed:
            reply = ""
            ret_val = test_import(status, "psutil")
        else:
            try:
                node_mem = psutil.virtual_memory()
            except:
                reply = ""
                status.add_error("Failed to retrieve memory info")
                ret_val = process_status.ERR_process_failure
            else:
                ret_val = process_status.SUCCESS
                reply = "\r\nTotal:     {0:,d}MB\r\n" \
                        "Available: {1:,d}MB\r\n" \
                        "Used:      {2:,d}MB\r\n" \
                        "Free:      {3:,d}MB\r\n".format(int(node_mem.total / 1024), int(node_mem.available / 1024),
                                                         int(node_mem.used / 1024), int(node_mem.free / 1024))
    return [ret_val, reply]
# ---------------------------------------------------------
# Get CPU info
# ---------------------------------------------------------
def get_cpu_info(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) != 3 or cmd_words[2] != "info":
        reply = ""
        ret_val = process_status.ERR_command_struct
    else:
        if not psutil_installed:
            reply = ""
            ret_val = test_import(status, "psutil")
        else:
            try:
                physical_cores = psutil.cpu_count(logical=False)
                logical_cores = psutil.cpu_count(logical=True)
            except:
                reply = ""
                status.add_error("Failed to retrieve CPU info")
                ret_val = process_status.ERR_process_failure
            else:
                reply = "\r\rPhysical Cores: %u" \
                        "\r\nLogical Cores:  %u" % (physical_cores, logical_cores)
                ret_val = process_status.SUCCESS
    return  [ret_val, reply]
# ---------------------------------------------------------
# Get CPU usage
# ---------------------------------------------------------
def get_cpu_usage(status, io_buff_in, cmd_words, trace):

    try:
        cpu_info = psutil.cpu_percent(percpu=True).copy()
    except:
        reply = ""
        status.add_error("Failed to retrieve CPU usage info")
        ret_val = process_status.ERR_process_failure
    else:
        cpu_per = 0
        reply_title = []
        for idx, usage in enumerate(cpu_info):
            cpu_per += usage
            reply_title.append(f"Core {idx+1}")
        cpu_per = cpu_per / len(cpu_info)  # Average on all CPUs
        cpu_per = round(cpu_per, 2)

        reply_title.append("Average")
        cpu_info.append(cpu_per)

        reply_list = [cpu_info]

        reply = utils_print.output_nested_lists(reply_list, "", reply_title, True)
        ret_val = process_status.SUCCESS

    return  [ret_val, reply]

# ----------------------------------------------------------------
# Get the list of active connections on the machine
# Command: get machine connections
# Command: get machine connections where port = 7850
# Details at https://psutil.readthedocs.io/en/latest/#psutil.net_connections
# ----------------------------------------------------------------
def get_machine_connections(status, io_buff_in, cmd_words, trace):

    if psutil_installed:

        title_columns = ["FD", "Family", "Type", "Local Address", "Remote Address", "Status", "PID"]

        words_count = len(cmd_words)
        if words_count == 7 and utils_data.is_sub_array(cmd_words, 3, ["where", "port", "="]):
            # Find the process using the port
            port = cmd_words[6]
            ret_val = process_status.SUCCESS
        elif words_count == 3:
            port = None       # Show all ports
            ret_val = process_status.SUCCESS
        else:
            info = ""
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            try:
                info_list = psutil.net_connections(kind='inet')
            except:
                info = ""
                status.add_error("Failed to retrieve socket connections")
                ret_val = process_status.ERR_process_failure
            else:
                if not port:
                    # SHow all
                    info = utils_print.output_nested_lists(info_list, "", title_columns, True)
                else:
                    # Show specific port
                    sub_list = []
                    for entry in info_list:
                        try:
                            if entry[3].port == int(port):
                                sub_list.append(entry)
                        except:
                            pass
                    info = utils_print.output_nested_lists(sub_list, "",title_columns, True)

    else:
        info = ""
        ret_val = test_import(status, "psutil")

    return [ret_val, info]

# ----------------------------------------------------------------
# Get the list of IPV4 and IPV6 addresses
# Command: get ip lIst
# ----------------------------------------------------------------
def get_all_ip_addresses(status, io_buff_in, cmd_words, trace):
    addresses = []

    if len(cmd_words) != 3 or cmd_words[2] != "list":
        info = ""
        ret_val = process_status.ERR_command_struct
    else:

        if psutil_installed:

            ret_val = process_status.SUCCESS
            ipv4s = list(get_ip_addresses(socket.AF_INET))

            for entry in ipv4s:
                new_ip = []
                addresses.append(new_ip)
                new_ip.append("IPv4")
                new_ip.append(entry[0])
                new_ip.append(entry[1])

            ipv6s = list(get_ip_addresses(socket.AF_INET6))
            for entry in ipv6s:
                new_ip = []
                addresses.append(new_ip)
                new_ip.append("IPv6")
                new_ip.append(entry[0])
                new_ip.append(entry[1])

            info = utils_print.output_nested_lists(addresses, "", ["Family", "Interface", "Address"], True)
        else:
            info = ""
            ret_val = test_import(status, "psutil")

    return [ret_val, info]
# ----------------------------------------------------------------
# Get the list IP addresses for the requested family
# ----------------------------------------------------------------
def get_ip_addresses(family):
    if psutil_installed:
        for interface, snics in psutil.net_if_addrs().items():
            for snic in snics:
                if snic.family == family:
                    yield (interface, snic.address)

    return None


# ----------------------------------------------------------------
# Get the CPU temperature
# Command: get temperature
# ----------------------------------------------------------------
def get_cpu_temperature(status, io_buff_in, cmd_words, trace):

    reply = ""
    if not psutil_installed:
        ret_val = test_import(status, "psutil")
    else:

        if cmd_words[1] == '=':
            offset = 2
        else:
            offset = 0

        if len(cmd_words) != (offset + 3):
            reply = ""
            ret_val = process_status.ERR_command_struct
        else:

            try:
                temperature = psutil.sensors_temperatures()
            except:
                reply = ""
                status.add_error("Failed to retrieve temperature info")
                ret_val = process_status.ERR_process_failure
            else:
                ret_val = process_status.SUCCESS
                reply = str(temperature)

    return [ret_val, reply]
# ----------------------------------------------------------------
# Get the CPU temperature
# Command: get temperature
# ----------------------------------------------------------------
def get_platform_info(status, io_buff_in, cmd_words, trace):

    try:
        platform_list = platform.uname()
    except:
        platform_info = "Not available"
    else:
        platform_info = "\r\nSystem:    %s" \
                        "\r\nVersion:   %s" \
                        "\r\nNode:      %s" \
                        "\r\nMachine:   %s" \
                        "\r\nProcessor: %s\r\n" \
                        % (platform_list.system, platform_list.version, platform_list.node, platform_list.machine, platform_list.processor)

    return [process_status.SUCCESS, platform_info]


# ----------------------------------------------------------------
# Get the CPU and memory usage for all processes
# Command: get os process
# Command: get os  process  anylog
# Command: get os  process [pid]
# Command: get os  process [all]
# Command: get os  process list
# ----------------------------------------------------------------
def get_os_process(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS

    process_list = []
    words_count = len(cmd_words)

    if words_count == 4 and cmd_words[3] == "list":
        # Only get the process list
        for proc in psutil.process_iter():
            pname = proc.name()
            pid = proc.pid
            process_list.append((proc.name(), proc.pid, proc.status()))

        process_list.sort()
        info = utils_print.output_nested_lists(process_list, "", ["Name", "ID", "Status"], True)
    else:
        # Get cpu info
        if words_count == 4 and cmd_words[3] == "all":
            # Iterate over all running processes

            try:
                cpu_count = psutil.cpu_count()
                counter = sum(1 for proc in psutil.process_iter())  # count processes

                for index, proc in enumerate(psutil.process_iter()):
                    # Get process detail as dictionary
                    ret_val, proccess_info = retrieve_process_info(status, proc, cpu_count)
                    if ret_val:
                        break
                    process_list.append(proccess_info)

                    utils_print.print_status(counter, 50, index)

            except:
                status.add_error("Failed to retrieve Process List")
                ret_val = process_status.ERR_process_failure

        elif words_count == 4 or words_count == 3:
            # get process info anylog
            # get process info pid
            try:
                cpu_count = psutil.cpu_count()
                if words_count == 3 or cmd_words[3].lower() == "anylog":
                    pid = os.getpid()       # Get AnyLog process ID
                else:
                    pid = int(cmd_words[3])
                proc = psutil.Process(pid)
            except:
                status.add_error("Failed to retrieve Process ID")
                ret_val = process_status.ERR_process_failure
            else:
                ret_val, proccess_info = retrieve_process_info(status, proc, cpu_count)
                if not ret_val:
                    process_list.append(proccess_info)
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            info = utils_print.output_nested_lists(process_list, "", ["ID", "Name", "Memory (MB)", "CPU (%)", "Status"], True)
        else:
            info = None

    return [ret_val, info]

# ----------------------------------------------------------------
# Get Info on each process
# ----------------------------------------------------------------
def retrieve_process_info(status, proc, cpu_count):

    try:
        process_dict = proc.as_dict(attrs=['pid', 'name', 'cpu_percent'])

        process_name = process_dict["name"]
        if process_name == "python.exe":
            process_name = "AnyLog"

        process_memory = round((proc.memory_info().vms / 1048576),2)  # (1024 * 1024)

        #cpu_perc = proc.cpu_percent(interval=1) or process_dict["cpu_percent"]
        cpu_perc =  round(proc.cpu_percent(interval=1),2)  # divide by the number of CPUs
        process_state = proc.status()
        info = [process_dict["pid"], process_name, process_memory, cpu_perc, process_state]
    except:
        status.add_error("Failed to retrieve Process Info")
        ret_val = process_status.ERR_process_failure
        info = None
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, info]

# ----------------------------------------------------------------
# Provide visualized monitoring
# It will use a dedicated thread, hitting enter stops the visualization
# Example continuous cpu operator query
# ----------------------------------------------------------------
def continuous_monitoring(status, io_buff_in, cmd_words, trace):

    global continuous_status_

    if not continuous_status_:
        if cmd_words[1].isdigit():
            # User time - optional - in seconds
            continuous_time = int(cmd_words[1])
            if len(cmd_words) <= 2:
                return process_status.ERR_command_struct
            cmd_offset = 2  # User command is after continuous time
        else:
            continuous_time = 5  # Wait 5 seconds - default
            cmd_offset = 1       # User command is after continuous

        # Start a thread to continuous (if not already running)
        t = threading.Thread(target=continuous_visualizing, args=("dummy", cmd_words, cmd_offset, continuous_time), name="Visualizer")
        t.start()
    else:
        utils_print.output("\n\rWait for termination of continuous process before initiating a new process", True)

    return process_status.SUCCESS

# ----------------------------------------------------------------
# Provide Continuous monitoring using a dedicated thread, until enter is entered
# ----------------------------------------------------------------
def continuous_visualizing(dummy, cmd_words, cmd_offset, continuous_time):

    global continuous_status_

    continuous_status_ = True       # Set true if continuous is running
    size = params.get_param("io_buff_size")
    buff_size = int(size)
    data_buffer = bytearray(buff_size)

    status = process_status.ProcessStat()
    enter_counter = get_enter_counter()

    # Loop until enter is hit

    lines_diff = 0

    utils_print.output("\n\n\r", False)

    exit_loop = False

    command_list = ' '.join(cmd_words[cmd_offset:]).split(',')

    while (enter_counter == get_enter_counter() ):

        if process_status.is_exit("", False):
            break       # Exit node was called

        print_str = ""
        listed_cmds = False
        for command in command_list:
            # Get the monitored functions
            listed_cmds = True
            sub_cmd_words = command.split()
            if sub_cmd_words[0] == "cpu":
                cmd_key = "cpu"
            else:
                cmd_key = "".join(sub_cmd_words)  # Remove spaces

            if cmd_key in commands_dict_:
                print_str += "\r\n"
                try:
                    print_str += commands_dict_[cmd_key](status, sub_cmd_words)    # Get the info
                except:
                    utils_print.output(f"Continuous using the command: '{command}' failed", True)
                    exit_loop = True
                    break
            else:
                # exec a single command continuously
                listed_cmds = False
                command = ' '.join(cmd_words[cmd_offset:])
                utils_print.output(f"\r\ncontinuous [{continuous_time} sec] {command}", True)
                ret_val = member_cmd.process_cmd(status, command, False, None, None, data_buffer)
                break


        if exit_loop or enter_counter != get_enter_counter():
            break

        if listed_cmds:
            utils_print.move_lines_up(lines_diff, True)     # And delete each line
            utils_print.output(print_str, False)
            lines_diff = print_str.count('\n')  # Count new lines to revert to the same starting place

        sleep_time = continuous_time
        # Avoid long sleep without exit by breaking to 10 seconds
        while sleep_time:
            if sleep_time <= 10:
                time.sleep(sleep_time)
                sleep_time = 0
            else:
                time.sleep(10)
                sleep_time -= 10
            if enter_counter != get_enter_counter():
                break
        if sleep_time:
            break

    continuous_status_ = False  # continuous not running
    utils_print.output("\n\rExit Continuous Monitoring", True)

# ----------------------------------------------------------------
# Maintain process statistics
# Update the statistics dictionary with runtime info
# ----------------------------------------------------------------
def update_process_statistics(stat_dict, is_operator, is_running):


    if is_operator:
        node_name =  metadata.get_node_operator_name()   # If operator
        if not node_name:
            node_name = get_node_name()
    else:
        node_name = get_node_name()

    stat_dict["Node name"] = node_name

    if not is_running:
        stat_dict["Status"] = "Not Active"
    else:

        stat_dict["Status"] = "Active"

        current_timestamp = utils_columns.get_current_time_in_sec()

        if "Start timestamp" in stat_dict:
            # Time since start
            start_timestamp = stat_dict["Start timestamp"]
            hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(current_timestamp - start_timestamp)
        else:
            # First call to this method
            stat_dict["Start timestamp"] = current_timestamp
            hours_time = minutes_time = seconds_time = 0

        stat_dict["Operational time"] = "%02u:%02u:%02u (H:M:S)" % (hours_time, minutes_time, seconds_time)

        if "Total events" in stat_dict and "New events" in stat_dict:
            # Update new events based on older events
            new_events = stat_dict["Total events"] - stat_dict["New events"]    # Calculate the difference
            stat_dict["New events"] = new_events
        else:
            stat_dict["Total events"] = 0
            stat_dict["New events"] = 0


        stat_dict["Query timestamp"] = current_timestamp

