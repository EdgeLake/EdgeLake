"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# ------------------------------------------------------------------------
# Setup:
# git config --global credential.helper 'cache --timeout=500000'
# Or git config --global credential.helper store
# run tcp server !ip 2048 and run rest server !ip 2049 0 and connect dbms sqlite !db_user !db_port system_query memory and debug on tcp
# For the Markdown - https://guides.github.com/features/mastering-markdown/
# ------------------------------------------------------------------------

import os
import sys
import time
import signal
import cython


trace_level = 0

file_path = __file__  # Path to this file

# Get info from system variables
# EDGELAKE_LIB is a path to an EdgeLake directory that includes the source
# EDGELAKE_HOME is a path to an EdgeLake directory that includes the user files
if sys.platform.startswith('win'):
    path_prefix = os.getenv("EDGELAKE_LIB")  # system variable to the AnyLog Code
    data_home = os.getenv("EDGELAKE_HOME")  # system variable to the AnyLog Data
else:
    path_prefix = os.environ.get("EDGELAKE_LIB")  # system variable to the AnyLog Code
    data_home = os.environ.get("EDGELAKE_HOME")  # system variable to the AnyLog Data

if trace_level:
    print(f"\n\rfile_path   from __file__:    '{file_path}'")
    print(f"\n\rpath_prefix from EDGELAKE_LIB:  '{path_prefix}'")
    print(f"\n\rdata_home   from EDGELAKE_HOME: '{data_home}'")

if path_prefix:
    # With system variables
    if path_prefix[-1] != '/' and path_prefix[-1] != '\\':
        path_prefix += '/'
    path_prefix += "EdgeLake/edge_lake/"
    if sys.platform.startswith('win'):
        path_prefix = path_prefix.replace('/', '\\')
else:
    # No system variables
    # Try to find the EdgeLake directory in the path to this file
    if file_path.rfind("EdgeLake") == -1:
        # this is not a complete path - add current path
        current_path = os.getcwd()
        if current_path[-1] != '/' and current_path[-1] != '\\' and file_path[0] != '/' and file_path[0] != '\\':
            file_path = current_path + '/' + file_path
        else:
            file_path = current_path + file_path

    index = file_path.rfind("edge_lake")
    if index >= 0:
        path_prefix = file_path[:index + 7]

    if sys.platform.startswith('win'):
        path_prefix = path_prefix.replace('/', '\\')

if not path_prefix:
    print(f"\nPath to 'EdgeLake' folder is not provided - define sys param: EDGELAKE_HOME")
    exit(-1)
else:
    index = path_prefix.rfind("EdgeLake")
if index > 0:
    index -= 1  # Skip last slash
if index > 0:
    if path_prefix[index - 1] == '/' or path_prefix[index - 1] == '\\':
        index -= 1  # Skeep second slash if available
if index >= 0:
    edgelake_path = path_prefix[:index]

api_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'api'))
generic_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'generic'))
tcpip_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'tcpip'))
cmd_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'cmd'))
dbms_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'dbms'))
json_sql_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'json_to_sql'))
job_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'job'))
blockchain_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'blockchain'))
members_dir = os.path.expanduser(os.path.expandvars(path_prefix + 'members'))

if sys.platform.startswith('win'):
    # Location for windows specific libraries if needed
    sys.path.insert(0, edgelake_path + "/EdgeLake/packages/windows")

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.generic.input_kbrd as input_kbrd
from edge_lake.generic.utils_columns import utc_delta
from edge_lake.generic.node_info import is_with_cli, get_license_message


try:
    signal.signal(signal.SIGINT, lambda x, y: None) # Suppress Ctrl+C KeyboardInterrupt tracebacks
except:
    pass


class UserInput:
    def __init__(self):

        member_cmd.max_command_words = member_cmd.set_max_cmd_words(member_cmd.commands)  # Returns the max words for each disctionary
        member_cmd.update_help_index(member_cmd.commands, "")
        member_cmd.sort_help_index()

        utc_delta()     # Time diff to utc

        if data_home:
            data_dir = data_home
        else:
            data_dir = os.getcwd()
            index = data_dir.rfind("EdgeLake")
            if index != -1:
                if index == 0:
                    data_dir = '.'
                elif data_dir[index - 1] == '/' or data_dir[index - 1] == '\\':
                    data_dir = data_dir[:index - 1]

        params.init(data_dir)  # initiate the user dictionary

        if cython.compiled or getattr(sys, 'frozen', False):
            version_type = "Release"
        else:
            version_type = "Source"

        status = process_status.ProcessStat()
        code_version = member_cmd.get_version(status, None, None, 0)

        utils_print.output(f"\n{code_version[1]} ({version_type})\r\n", False)
        if version_type == "Release":
            utils_print.output(get_license_message("eval"), True)
        utils_print.output("\r\n", True)

        job_scheduler.initiate()


    def process_input(self, arguments = 0, arguments_list = None):

        member_cmd.workers_pool = utils_threads.WorkersPool("Query", 3)  # Default setup for workers supporting queries

        size = params.get_param("io_buff_size")
        buff_size = int(size)
        data_buffer = bytearray(buff_size)
        status = process_status.ProcessStat()
        status.set_error_dest("stdout")  # error messages send to output

        ret_value = start_init_commands(status, data_buffer)
        if ret_value:
            utils_print.output_box("Failed to execute init commands")
            return process_status.EXIT

        in_command = ""

        quotation = False
        for i in range(1, arguments):

            cmd_word = arguments_list[i]
            if cmd_word[0] == '"' and not quotation:
                # consider all in quotation as one command
                quotation = True
                if len (cmd_word) == 1:
                    continue        # Only quotation
                cmd_word = cmd_word[1:] # remove quotation
            elif quotation and cmd_word[-1] == '"':
                # end quotation
                quotation = False
                if len (cmd_word) == 1:
                    continue        # Only quotation

            if not quotation and cmd_word == "and":
                in_command = in_command + "& "          # Split between commands
            else:
                in_command = in_command + cmd_word + ' '

        if arguments > 1:  # execute command line arguments

            index = in_command.find("#")
            if index != -1:
                in_command = in_command[:index]     # remove comment

            if len(in_command):
                in_command.strip() # removing any leading and trailing whitespaces

            if len(in_command):
                # list the commands to execute
                commads_list = in_command.split('&')

                for command in commads_list:  # process command line argument

                    if not command:
                        continue

                    if command[0] == '#':
                        break  # comment out all remaining commands on the command line

                    status.reset()

                    utils_print.output(command.lstrip(), True)
                    if (member_cmd.process_cmd(status, command=command, print_cmd=False, source_ip="", source_port="",
                                               io_buffer_in=data_buffer) == -1):  # -1 is an exit command
                        return


        if not is_with_cli():
            # this is a background process - ignore input
            utils_print.output_box("AnyLog CLI disabled")
            wait_to_exit()
            return

        # execute user input
        new_line_counter = 0

        params.save_input_thread()  # save the name of this thread as the thread doing the keyboard input

        keyboard = input_kbrd.Keyboard()
        cmd_buffer = ""
        interactive = True
        place_in_buff = True

        while (1):

            if new_line_counter == utils_print.get_new_line_counter():
                utils_print.print_prompt()

            user_input = keyboard.get_cmd(place_in_buff)

            if not len(user_input):
                # test interactive terminal and connected to stdin
                if interactive:
                    utils_print.output_box(
                        "AnyLog Node in Detached Mode - For Interactive Mode, attach the console to the standard input, output")
                interactive = False
                time.sleep(5)
                continue

            if not cmd_buffer:
                # remove leading spaces to catch a start of a code block
                in_command = user_input.lstrip(' ')     # Needs to be done after the test for interactive mode
                if not in_command:
                    continue
            else:
                in_command = user_input

            interactive = True

            new_line_counter = utils_print.get_new_line_counter()

            # Support a string in multiple lines - with brackets: [multi-line command]
            if in_command[0] == '<':  # make one command from multiple rows (code block)
                place_in_buff = False  # Code block is not repeatable by keyboard up arrow
                cmd_buffer = in_command[:-1].strip()  # remove end of line
                if in_command[-2] != '>':
                    new_line_counter += 1  # Avoid print of prompt
                    continue  # get next line
                in_command = cmd_buffer[1:-1]
                cmd_buffer = ""
            elif cmd_buffer:
                if len(in_command) == 1:
                    new_line_counter += 1  # Avoid print of prompt
                    continue  # nul row (only enter)
                cmd_buffer += " " + in_command[:-1].strip()
                if cmd_buffer[-1] == '>':
                    in_command = cmd_buffer[1:-1]  # all the input string is available - process message
                    cmd_buffer = ""
                else:
                    new_line_counter += 1  # Avoid print of prompt
                    continue  # get next line

            place_in_buff = True        # Place the next keyboard command in a buffer for reuse

            commads_list = in_command.split('&')

            for command in commads_list:  # process command line argument

                if command == '\n':
                    continue

                status.reset()

                ret_value = member_cmd.process_cmd(status=status, command=command, print_cmd=False, source_ip="",
                                                   source_port="", io_buffer_in=data_buffer, user_input = True)

                if ret_value == process_status.EXIT:
                    return


# =======================================================
# PLace the main thread on a wait to exit
# =======================================================
def wait_to_exit():
    while True:
        if process_status.is_exit("all", True):
            return
        time.sleep(5)       # wait 5 seconds

# =======================================================
# Start sys commands
# =======================================================
def start_init_commands(status, data_buffer):
    # start a thread to track the failed nodes in the metadata
    commands = [

        "schedule time = 1 minute and scheduler = 0 and name = \"Metadata Ping\" task set servers ping",        # start a thread to track non responsive nodes

        "run scheduler 0"       # Start the sys scheduler
    ]
    for command in commands:
        ret_value = member_cmd.process_cmd(status=status, command=command, print_cmd=False, source_ip="",
                                           source_port="", io_buffer_in=data_buffer)
        if ret_value:
            break
    return ret_value

