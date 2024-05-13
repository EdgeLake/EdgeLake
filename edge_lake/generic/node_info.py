"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import sys
import configparser
import importlib

import edge_lake.generic.process_status as process_status
import edge_lake.tcpip.tcpip_server as tcpip_server
import edge_lake.tcpip.net_utils as net_utils

try:
    CONFIG_FILE = os.path.expandvars(os.path.expanduser(os.path.join('$EDGELAKE_HOME', 'setup.cfg')))
    SOURCE_CONFIG_FILE = os.path.join(os.path.abspath(__file__).split("edge_lake")[0],  'setup.cfg')
except:
    CONFIG_FILE = None
    SOURCE_CONFIG_FILE = None

copyright_notice_ =  "\n\r* (c) 2021-2023 AnyLog Inc.\r\n*\n"
license_eval_notice_ =    \
                    "* This software is licensed under the terms and conditions of the AnyLog SOFTWARE EVALUATION AGREEMENT\r\n" \
                    "* available at https://github.com/AnyLog-co/documentation/blob/master/License/Evaluation%20License.md \r\n" \


node_name_ = ""
queue_msg_ = False      # Changed to True if messages are in the queue

peer_nodes_ = ""         # This value is updated with target nodes

cli_mode_ = True        # With Off , CLI is disabled

no_assigned_cli_ = {        # Commands that are not send using the remote CLI
    "help"        :         None,
    "wait"        :         None,
    "blockchain" :          {
                                "insert": None,  # Only on current node
                            },
    "seed"        :         None,
    "run"        :          {
                                "client": None,  # Only on current node
                            }
}

# =======================================================================================================================
# is with destination - commands are executed locally even with assigned CLI
# =======================================================================================================================
def is_with_destination(cmd_words):

    if peer_nodes_:
        # is_command_included returns True if commands are in the no_assigned_cli_
        return not is_command_included(cmd_words, 0, no_assigned_cli_)

    return False

# =======================================================
# Test if the command is included in the structure provided
# =======================================================
def is_command_included(cmd_words, offset_first_word, structure):

    first_word = cmd_words[offset_first_word]
    ret_val = False
    if first_word in structure:
        second_dict = structure[first_word]
        if not second_dict:
            # all commands allowed, for example - exit ...
            ret_val = True
        else:
            words_count = len(cmd_words) - offset_first_word
            if words_count >= 2:
                second_word = cmd_words[offset_first_word + 1]
                if second_word in second_dict:
                    # Test the list
                    words_list = second_dict[second_word]
                    if not words_list:
                        ret_val = True      # No need to continue the test
                    else:
                        start_offset = words_list[0]    # The location to validate
                        end_offset = start_offset + len(words_list) - 1
                        if words_count >= end_offset:
                            ret_val = True
                            for index in range(start_offset, end_offset):
                                if words_list[index -start_offset + 1] != cmd_words[offset_first_word + index]:
                                    ret_val = False
                                    break

    return ret_val


# =======================================================
# Add destination to command
# =======================================================
def add_destination(command_words, words_count, offset_first):

    if words_count > (2 + offset_first) and command_words[offset_first] == "run" and command_words[offset_first + 1] == "client":
        use_dest = False  # Run client is issued from the current node
    elif words_count == 1:
        use_dest = False        # Examples: !value, help
    elif words_count == 3 and command_words[1] == '=':
        # assignment
        use_dest = False
    else:
        use_dest = True     # add Destination


    if use_dest:
        # Add destination
        if not offset_first:
            # No assignment
            new_cmd = ["run", "client", get_destination(),] + command_words
        elif offset_first == 2:
            # Keep assignment at the prefix
            new_cmd = [command_words[0], "=", "run", "client", get_destination(), ] + command_words[2:]
    else:
        new_cmd = command_words   # Unchanged
    return new_cmd

# =======================================================================================================================
# get destination for CLI command
# =======================================================================================================================
def get_destination():
    global peer_nodes_
    if len(peer_nodes_):
        dest = f"{peer_nodes_}" if  peer_nodes_[0] == '(' else f"({peer_nodes_})"
    else:
        dest = ""
    return dest


# =======================================================================================================================
# Set an assigned CLI
# Set the peer node or nodes that will be the target of the CLI commands
# =======================================================================================================================
def set_assigned_cli(target_nodes):
    global peer_nodes_

    if target_nodes == '.':
        peer_nodes_ = ""    # Cancell the assigned CLI
        return process_status.SUCCESS

    if target_nodes[0] != '(' or target_nodes[-1] != ')':
        return process_status.ERR_command_struct

    if len(target_nodes) == 2 or (target_nodes[1] == ' ' and len(target_nodes[1:-1].replace(' ',''))):  # test the first char to do the replace
        # Empty parenthesis
        return process_status.ERR_command_struct

    peer_nodes_ = target_nodes[1:-1].strip()

    return process_status.SUCCESS

# =======================================================================================================================
# Return the prompt
# =======================================================================================================================
def get_prompt():

    name = node_name_

    if queue_msg_:
        # Changed to True with unread messages -> show the + sign
        if name:
            prompt = "\rAL " + name + " +> "
        else:
            prompt = "\rAL +> "       # Add + sign for messages is queue
    else:
        if name:
            prompt = "\rAL " + name + " > "
        else:
            prompt = "\rEL > "

    if peer_nodes_:
        prompt += (peer_nodes_ + " >> ")

    return prompt

# ----------------------------------------------------------------------
# Set the node name
# ----------------------------------------------------------------------
def set_node_name(node_name):
    global node_name_
    node_name_ = node_name
    return process_status.SUCCESS
# ----------------------------------------------------------------------
# Get the node name - reply to user command
# ----------------------------------------------------------------------
def get_current_node_name(status, io_buff_in, cmd_words, trace):
    node_name = get_node_name()
    return [process_status.SUCCESS, node_name]
# ----------------------------------------------------------------------
# Get the node name
# ----------------------------------------------------------------------
def get_node_name():
    global node_name_

    name = node_name_ if node_name_ else "AnyLog"

    node_id = net_utils.get_external_ip_port()
    if not node_id:
        node_id = tcpip_server.get_ip()

    return name + '@' + node_id

# ----------------------------------------------------------------------
# The AnyLog Version
# ----------------------------------------------------------------------
def get_version(status):
    """
    Get the AnyLog version from setup.cfg
    :global:
       CONFIG_FILE:str - path for Docker image
       SOURCE_CONFIG_FILE:str - path for source deployment
       DEFAULT_VERSION:str - default version
    :params:
        anylog_version:str - AnyLog version
    :return:
        AnyLog version either from setup.cfg or default
    """
    anylog_version = None
    config = configparser.ConfigParser()

    if os.path.isfile(CONFIG_FILE):
        file_name = CONFIG_FILE
        try:
            config.read(CONFIG_FILE)
            anylog_version = config['metadata']['version']
        except:
            errno, errval = sys.exc_info()[:2]

    elif os.path.isfile(SOURCE_CONFIG_FILE):
        file_name = SOURCE_CONFIG_FILE
        try:
            config.read(SOURCE_CONFIG_FILE)
            anylog_version = config['metadata']['version']
        except:
            errno, errval = sys.exc_info()[:2]
    else:
        file_name = ""
        errno = 0
        errval = ""

    if anylog_version is None:
        anylog_version = 0
        status.add_error(f"Failed to retrieve version from file at: '{file_name}' errno: {errno} errval: {errval}")

    return anylog_version
# ----------------------------------------------------------------------
# Get license Message
# ----------------------------------------------------------------------
def get_license_message(license_type):
    msg = copyright_notice_
    if license_type == "eval":
        msg += license_eval_notice_
    return msg

# ----------------------------------------------------------------------
# Test is a foreground or background process
'''
os.getpgrp() returns the process group ID of the current process.
os.tcgetpgrp(0) returns the process group ID of the terminal associated with file descriptor 0 (usually the standard input).
    If these two values are different, it suggests that the script is running in the background because 
    the process group IDs are different when a process is in the background compared to when it's in the foreground.
'''
# Example to running in the background:
# Linux
# nohup python3 -u anylog.py process start.al &
# Windows
# start "" pythonw.exe anylog.py
# ----------------------------------------------------------------------
def is_foregound():

    try:
        if sys.platform.startswith('win'):
            if sys.stdout.isatty():
                ret_val = True      # Running in the foreground
            else:
                ret_val = False     # Running in the background
        else:
            if os.getpgrp() == os.tcgetpgrp(0):
                ret_val = True      # Running in the foreground
            else:
                ret_val = False
    except:
        ret_val = True      # The defaul
    return ret_val

# ----------------------------------------------------------------------
# Disable the CLI when AnyLog is a background process
# set cli off
# ----------------------------------------------------------------------
def set_cli(status, io_buff_in, cmd_words, trace):
    global cli_mode_

    if cmd_words[2] == "off":
        cli_mode_ = False
        ret_val = process_status.SUCCESS
    elif cmd_words[2] == "on":
        cli_mode_ = True
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.Error_command_params
    return ret_val

# ----------------------------------------------------------------------
# Disable the CLI when AnyLog is a background process
# set cli off
# nohup python3 -u anylog.py process start.al &
# View background:
#   pgrep -f "anylog.py"
#   pgrep -af "python"
# ----------------------------------------------------------------------
def is_with_cli():
    global cli_mode_
    return cli_mode_
# ----------------------------------------------------------------------
# Test failed import - and provide a failure message
# This message is used to get the error when an import fails
# ----------------------------------------------------------------------
def test_import(status, module_name):

    try:
        module = importlib.import_module(module_name)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to import {module_name}: (Error: ({errno}) {value})")
    else:
        status.add_error(f"Success in import {module_name} (second attempt)")


    return process_status.Failed_to_import_lib       # Returned regardless if import failed or not