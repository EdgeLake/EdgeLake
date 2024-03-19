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
import sys
import socket
import struct
import subprocess
import errno
import ipaddress
from time import sleep
import traceback  # Enable for stacktrace

import anylog_node.generic.utils_print as utils_print
import anylog_node.generic.utils_json as utils_json
import anylog_node.generic.process_status as process_status
import anylog_node.tcpip.message_header as message_header
import anylog_node.generic.process_log as process_log
import anylog_node.generic.params  as params
import anylog_node.cmd.member_cmd as member_cmd
import anylog_node.generic.interpreter as interpreter

active_connections_ = [
    #           External            Local    bind address
    ["TCP",         None,           None,     None   ],          # Network connections
    ["REST",        None,           None,     None   ],          # Rest connections
    ["Messaging",   None,           None,     None   ],          # MQTT connections
    #           connection is represented by 3 fields: IP:Port,  IP,     Port
]

reply_addr_ = None # A value on the reply IP will direct the message to this addr - or 0.0.0.0 - the reply addr is taken from the socket
reply_port_ = None

self_addr_ = None    # An IP to use when the node sends a message to itself
self_port_ = None

BROKEN_PIPE = 32

TCP_DEBUG = False

use_ipv4_ = False       # True tests IPV4 values


default_ports_ = {
    "master" : (32048, 32049, 32050),
    "query" :  (32348, 32349, 32350),
    "operator" : (32148, 32149, 32150),
    "publisher" : (32248, 32249, 32250)
}
# ----------------------------------------------------------------
# Get the connection info
# ----------------------------------------------------------------
def get_existing_connection(server_type):

    service_connections = active_connections_[server_type]      # 0 --> TCP, 1 --> REST, 2 --> MSG
    if service_connections[1]:
        external_ip = service_connections[1][1]
        external_port = service_connections[1][2]
    else:
        external_ip = ""
        external_port = 0

    if service_connections[2]:
        local_ip = service_connections[2][1]
        local_port = service_connections[2][2]
    else:
        local_ip = ""
        local_port = 0
    if service_connections[3]:
        # if value which does not start with 0.0 - this is a real address to bind
        is_bind = False if service_connections[3][0].startswith("0.0") else True
    else:
        is_bind = False

    return [external_ip, external_port, local_ip, local_port, is_bind]

# ----------------------------------------------------------------
# Test if the input is identical to the current connection
# ----------------------------------------------------------------
def is_new_connections(server_type, main_ip, main_port, second_ip, second_port, bind):

    external_ip, external_port, local_ip, local_port, is_bind = get_existing_connection(server_type)

    if external_ip != main_ip or external_port != main_port or \
            second_ip != local_ip or second_port != local_port or \
            bind != is_bind:

        return True

    return False


# ----------------------------------------------------------------
# Get the number of threads from config policy
# ----------------------------------------------------------------
def get_threads_from_ploicy(status, policy_id, json_object, threads_type, default_value):
    # threads_type - "tcp, rest, msg whiich is extended to the keys: tcp_threads, rest_threads and msg_threads

    ret_val = process_status.SUCCESS
    policy_key = f"{threads_type}_threads"
    tcp_threads = json_object[policy_key] if threads_type in json_object else default_value
    try:
        tcp_threads = int(tcp_threads)
        if not tcp_threads:
            tcp_threads = default_value # Replace 0 with default value
    except:
        tcp_threads = 0
        status.add_error(f"Wrong number of TCP Threads in config policy: '{policy_id}'")
        ret_val = process_status.ERR_command_struct

    return [ret_val, tcp_threads]

# ----------------------------------------------------------------
# Init Message Server
# connect_name - "TCP Server", "Message Broker"
# exit_name - the name to test for exit: "tcp", "broker"
# workers_pool - threads to process messages
# receive_data - the method for the threads to execute
# ----------------------------------------------------------------
def message_server(connect_name:str, exit_name:str, host: str, port: int, buff_size: int, workers_pool, rceive_data, is_bind, trace):

    try:
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        error_msg = connect_name + ": Failed to create a socket - " + str(sys.exc_info())
        process_log.add_and_print("Error", error_msg)
        return

    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)  # SO_REUSEADDR - ignoe OS timeout for the socket
    soc.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)  # alawys send the data
    # socket created

    soc.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 20 * buff_size)
    soc_buff_size = soc.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

    ret_val = True

    if is_bind:
        bind_host = host    # Listen to a single IP in the machine
    else:
        bind_host = ""      # Listen to all IPs with the provided port
    try:
        soc.bind((bind_host, port))      #  soc.bind((host, port))
    except:
        err_msg = connect_name + ": Bind failed error - " + str(sys.exc_info())
        process_log.add_and_print("Error", err_msg)
        ret_val = False

    if ret_val:  # was able to Bind

        # put the socket into listening mode
        soc.listen(5)

        process_log.add("Event", "%s initiated on %s:%u with socket buffer size: %u" % (connect_name, host, port, soc_buff_size))

        # a forever loop until client wants to exit

        soc.settimeout(3)

        while True:

            try:
                # establish connection with client
                clientSoc, ClientAddr = soc.accept()

            except:
                if process_status.is_exit(exit_name):
                    break
                errno, value = sys.exc_info()[:2]
                if errno == socket.timeout:
                    continue
                err_msg = "%s: Error socket accept: {0} : {1}".format(connect_name, str(errno), str(value))
                ret_val = False
                break

            if process_status.is_exit(exit_name):
                break

            # Provide the socket to a new thread
            task = workers_pool.get_free_task()
            task.set_cmd(client_thread, [clientSoc, ClientAddr, connect_name, rceive_data, buff_size])
            workers_pool.add_new_task(task)

        try:
            soc.close()
        except:
            pass

        workers_pool.exit()

    if not ret_val:
        process_log.add_and_print("error", "Terminating %s %s:%u with message: %s" % (connect_name, host, port, err_msg))
    else:
        process_log.add_and_print("event", "Terminating %s %s:%u" % (connect_name, host, port))

# =======================================================================================================================
# Exit one of the servers (tcp/rest/broker), wait for threads terminating
# =======================================================================================================================
def wait_for_exit_server(status, server_name, workers_pool):

    counter = 0
    while workers_pool.all_threads_terminated() == False:
        sleep(1)  # wait for all threads to exit
        counter += 1
        if counter > 10:
            status.add_error("Failed to terminate %s threads" % server_name)
            return process_status.ERR_process_failure # If some unknown error
    return process_status.SUCCESS
# ----------------------------------------------------------------
# Run a thread against the client
# rceive_data is the method to get the data using the IP/Port and process the call
# ----------------------------------------------------------------
def client_thread(status, data_buffer, clientSoc, ClientAddr, connect_name, rceive_data, buff_size):

    ip_in, port_in = str(ClientAddr[0]), str(ClientAddr[1])

    if member_cmd.is_debug_method("exception"):
        rceive_data(status, data_buffer, params, clientSoc, ip_in, port_in, buff_size)
    else:

        try:
            # timeout would break
            rceive_data(status, data_buffer, params, clientSoc, ip_in, port_in, buff_size)
        except Exception as e:
            # stack_data = traceback.format_exc()   # Enable import traceback
            # utils_print.output(stack_data, True)
            stack_data = traceback.format_exc()
            err_msg = "%s got error while processing message: %s" % (connect_name, str(e))
            status.add_error(err_msg)
            utils_print.output(err_msg, True)
            utils_print.output(stack_data, True)

            ret_val = process_status.ERR_process_failure

    try:
        clientSoc.shutdown(1)
    except:
        pass

    try:
        clientSoc.close()
    except:
        pass

# =======================================================================================================================
# Get servers params from the command words:
# An updated version with additional options
# Example: run tcp server where internal_ip = !ip and internal_port = !port and external_ip = !external_ip and external_port = !extenal_port and bind = !is_bind and workers = !workers_count
# =======================================================================================================================
def get_config_server_params(status, server_name, cmd_words, default_workers):

    #                                Must     Add      Is
    #                                exists   Counter  Unique
    keywords = {"internal_ip": ("str", False, False, True),
                "internal_port": ("int", False, True, True),
                "external_ip": ("str", False, False, True),
                "external_port": ("int", False, True, True),
                "threads": ("int", False, False, True),
                "bind": ("bool", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if not ret_val:
        # conditions satisfied by keywords

        if not counter:
            status.add_error(f"Missing port in {server_name} server configuration")
            ret_val = process_status.Error_command_params

    external_ip, external_port, workers, bind = \
            interpreter.get_multiple_values(conditions,
                                                       ["external_ip", "external_port",  "workers",         "bind"],
                                                       [None,          None,             default_workers,   False])

    internal_ip = interpreter.get_one_value_or_default(conditions, "internal_ip", external_ip)  # Must have a value
    internal_port = interpreter.get_one_value_or_default(conditions, "internal_port", external_port)  # Must have a value

    return [ret_val, external_ip, external_port, internal_ip, internal_port, workers, bind]

# =======================================================================================================================
# Get socket info
# =======================================================================================================================
def get_tcp_info(soc):
    soc_info = soc.getsockname()
    peer_info = soc.getpeername()

    info_string = "Socket IP: %s Socket Port: %u Peer IP: %s Peer Port: %u" % (
    soc_info[0], soc_info[1], peer_info[0], peer_info[1])

    utils_print.output(info_string, True)

    fmt = "B" * 7 + "I" * 21
    info = struct.unpack(fmt, soc.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 92))

    utils_print.output(info, True)

    buffsize = soc.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    utils_print.output("Socket buff size: %u" % buffsize, True)


# =======================================================================================================================
# Get socket status using netstat
# =======================================================================================================================
def get_soc_stat(soc, message, output):
    # name1 = soc.gethostname()
    soc_info = soc.getsockname()

    proc = subprocess.Popen(["netstat -an | grep %u" % soc_info[1]], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()

    out_msg = message + ":\n" + out.decode('ascii')

    if output:
        utils_print.output(out_msg, True)

    return out_msg

# =======================================================================================================================
# Get a port number as f(node type and available ports)
# get unused [tcp/rest/msg] port for [node type]
# get unused tcp port for operator
# =======================================================================================================================
def get_available_port(status, io_buff_in, cmd_words, trace):

    service_type = cmd_words[2]
    if service_type == "tcp":
        service_index = 0
    elif service_type == "rest":
        service_index = 1
    elif service_type == "msg":
        service_index = 2
    else:
        return [process_status.ERR_command_struct, None]


    if  cmd_words[3] != "port" or cmd_words[4] != "for":
        return [process_status.ERR_command_struct, None]

    node_type = cmd_words[5]
    if not node_type in default_ports_:
        status.add_error(f"No default ports available for node type '{node_type}'")
        return [process_status.ERR_command_struct, None]

    start_port = default_ports_[node_type][service_index]

    ret_val, port =  get_unused_port(status, node_type, start_port)
    return [ret_val, str(port)]
# =======================================================================================================================
# Get unused port from a starting IP. Increase the port number by 10 to maintain similarity to the defaults.
# For example - Operator will try - 32148, 32158, 32168 etc
# =======================================================================================================================
def get_unused_port( status, policy_type, start_port ):

    test_port = start_port
    counter = 0

    ret_val = process_status.SUCCESS
    while not is_port_open('127.0.0.1', test_port):
        counter += 1
        if counter > 10:
            status.add_error(f"Failed to identify unused port for {policy_type} starting at {start_port}")
            ret_val = process_status.Failed_to_assign_unused_port
            break
        test_port += 10

    return [ret_val, test_port]

# =======================================================================================================================
# check if a port is open
# Use 'localhost' for the local machine
# =======================================================================================================================
def is_port_open(host, port):
    try:
        # Create a socket object
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Set a timeout for the connection attempt
            s.settimeout(5)
            # Attempt to connect to the host and port
            conn = s.connect_ex((host, port))
        return True
    except:
        errno, value = sys.exc_info()[:2]
        return False


# =======================================================================================================================
# Get the socket error
# =======================================================================================================================
def get_socket_error(soc, output, txt):
    err_val = soc.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    if err_val:
        if err_val in errno.errorcode.keys():
            err_message = errno.errorcode[err_val]
        else:
            err_message = "Unknown"

        msg = "%s: Soket Error: %u  message: %s" % (txt, err_val, err_message)
        if output:
            utils_print.output(msg, True)
    else:
        msg = ""  # not an error

    return msg


# =======================================================================================================================
# Get the local host name
# =======================================================================================================================
def get_local_host_name(status, io_buff_in, cmd_words, trace):
    try:
        host_name = socket.gethostname()
    except:
        formatted_name = ""
        ret_val = process_status.Failed_host_name
    else:
        formatted_name = "{}".format(host_name)
        ret_val = process_status.SUCCESS

    return [ret_val, formatted_name]


# =======================================================================================================================
# Set IPv4 format and port
# =======================================================================================================================
def test_ipv4(ip_addr, port_str:str):

    global use_ipv4_


    if not port_str.isdecimal():
        process_log.add("Error", "Errror in port value at address: %s:%s" %(ip_addr, port_str))
        return False

    port = int(port_str)

    if port == 0 or port > 65535:
        err_msg = "Port value %u is outside range (1-65535)" % port
        process_log.add("Error", err_msg)
        return False

    if not ip_addr:
        err_msg = "Missing IP address"
        process_log.add("Error", err_msg)
        return False

    if not use_ipv4_:       # DO not test the IP
        return True

    ip_segments = ip_addr.split('.')

    if len(ip_segments) != 4:
        err_msg = "IP address '%s' is with wrong IPv4 format" % ip_addr
        process_log.add("Error", err_msg)
        return False

    for entry in ip_segments:
        if not entry.isnumeric():
            err_msg = "IP address '%s' is with wrong IPv4 format" % ip_addr
            process_log.add("Error", err_msg)
            return False
        if int(entry) > 255:
            err_msg = "IP address '%s' is with wrong IPv4 format - value %s is outside range" % (ip_addr, entry)
            process_log.add("Error", err_msg)
            return False
    return True

# ---------------------------------------------------------
# Test the IP and port string
# ---------------------------------------------------------
def is_valid_ip_port(ip_port):
    if not ip_port:
        return False
    ip_port = ip_port.split(':')
    if len(ip_port) != 2:
        return False
    if not ip_port[1].isnumeric():
        return False
    return test_ipaddr(ip_port[0], int(ip_port[1]))


# =======================================================================================================================
# Test IPv4 and IPv6 formats and port value
# =======================================================================================================================
def test_ipaddr(ip_addr: str, port: int):

    # If port is 0, we ignore the test although the value is wrong
    if port > 65535:
        err_msg = "Port value %u is outside range (1-65535)" % port
        process_log.add("Error", err_msg)
        return False

    #if re.match('^[0-9\.]*$',ip_addr):  # Added because Kubernetes virtual addresses fail i.e.: anylog-svs2.default.svc.cluster.local

    return is_valid_ip_addr(ip_addr)

# =======================================================================================================================
# Test if IP is valid
# =======================================================================================================================
def is_valid_ip_addr(ip_addr):
    try:
        ipaddress.ip_address(ip_addr)
    except:
        ret_val = False
    else:
        ret_val = True
    return ret_val

# =======================================================================================================================
# Is message source - return True is this node is the source of the message:
# If a message send from node A to node B and a reply is returned to node A (A-->B-->A)
# Node A returns True and node B returns false
# =======================================================================================================================
def is_source_node(mem_view):

    ip, port = message_header.get_source_ip_port(mem_view)

    source_ip_port = ip + ":" + str(port)

    return  (source_ip_port == get_external_ip_port() or source_ip_port == get_local_ip_port())

# =======================================================================================================================
# Set the IP and port of the destination node in the message header
# 2 options:
# a) if the destination is on my local network - place the local network IP
# b) if the destination is on external network - place the external network IP
# =======================================================================================================================
def set_ip_port_in_header(status, mem_view, destination_ip, destination_port):

    global active_connections_
    global reply_addr_
    global reply_port_

    if reply_addr_:
        # User command: set reply ip = x, x can be "dynamic" to set it on 0.0.0.0
        # The user determined the reply address. If reply address is set to 0.0.0.0 - the reply is determined by the socket at the destination node
        reply_ip = reply_addr_
        if reply_port_:
            reply_port = reply_port_
        else:
            reply_port = get_external_port()
    elif is_local_network(destination_ip):
        # sending a message to a different node but on the same network ->  set the reply IP as the local IP
        reply_ip = get_local_ip()
        reply_port = get_local_port()

    else:
        reply_ip = get_external_ip()
        reply_port = get_external_port()

    if not reply_ip or not reply_port:
        if status:
            status.add_error("TCP server is not running")
        ret_val = process_status.ERR_source_IP_Ports

    elif not message_header.set_source_ip_port(mem_view, reply_ip, reply_port):
        if status:
            status.add_error("IP address is larger than max length")
        ret_va = process_status.ERR_dest_IP_Ports
    else:
        ret_val = process_status.SUCCESS

    return ret_val
# ---------------------------------------------------------
# Get the destination IP and Port by determining if the sender is on the same local network.
# In that case, use the local IP and Port.
# Given an opbject with IP, Port, Local IP and Local Port -
# Determine which IP and Port to use
# ---------------------------------------------------------
def get_dest_ip_port(dest_object):

    if "ip" in dest_object:
        external_ip = dest_object["ip"]
        if is_local_network(external_ip):
            # The ip is a local network - use the assigned port
            if "port" in dest_object:
                ip = external_ip
                port = dest_object["port"]
            else:
                ip = None
                port = None
        else:
            # Destination is not a local address
            if external_ip == get_external_ip():
                # This node and destination node share the same IP - use the local IP
                local_ip = get_local_ip()
                if "local_ip" in dest_object:
                    ip = dest_object["local_ip"]
                else:
                    ip = local_ip
                if "port" in dest_object:
                    external_port = dest_object["port"]
                    if ip == local_ip and external_port == get_external_port():
                        # This message is send and receiver is the same node - use the local ip and port
                        port = get_local_port()
                    else:
                        if "local_port" in dest_object:
                            port = dest_object["local_port"]
                        else:
                            port = external_port
                else:
                    # Missing "port" value
                    ip = None
                    port = None
            else:
                if "port" in dest_object:
                    ip = external_ip
                    port = dest_object["port"]
                else:
                    ip = None
                    port = None
    else:
        # No IP attribute - use local ip if available
        if "local_ip" in dest_object:
            ip = dest_object["local_ip"]
            if "local_port" in dest_object:
                port = dest_object["local_port"]
            elif "port" in dest_object:
                port = dest_object["port"]
            else:
                # no port and no local_port
                ip = None
                port = None
        else:
            # no IP and no local_ip
            ip = None
            port = None
    return [ip, port]

# ---------------------------------------------------------
# Wait for a server declaration for the specified seconds
# ---------------------------------------------------------
def wait_for_server( status, max_time ):
    global active_connections_

    wait_time = 0
    ret_val = process_status.SUCCESS
    while not active_connections_[0][1]:
        sleep(2)
        wait_time += 2
        if wait_time >= max_time:
            if not active_connections_[0][1]:
                status.add_error("TCP server is not running")
                ret_val = process_status.TCP_not_running    # TCP server is not up after the wait time expired
            else:
                ret_val = process_status.SUCCESS      # TCP Server is up
            break
    return ret_val

# ----------------------------------------------------------------
# Set debug mode to True / False
# ----------------------------------------------------------------
def set_tcp_debug(state):
    global TCP_DEBUG
    TCP_DEBUG = state

# ----------------------------------------------------------------
# Is debug mode to True / False
# ----------------------------------------------------------------
def get_TCP_debug():
    global TCP_DEBUG
    return TCP_DEBUG


# ---------------------------------------------------------
# Get the local IP and port
# ---------------------------------------------------------
def get_local_ip_port():

    global active_connections_

    if active_connections_[0][2]:
        ip_port = active_connections_[0][2][0]   # Get IP and Port of the local network
    else:
        ip_port = None

    return ip_port
# ---------------------------------------------------------
# Get the local IP
# ---------------------------------------------------------
def get_local_ip():

    global active_connections_

    if active_connections_[0][2]:
        ip = active_connections_[0][2][1]   # Get IP and Port of the local network
    else:
        ip = None

    return ip
# ---------------------------------------------------------
# Get the local port
# ---------------------------------------------------------
def get_local_port():

    global active_connections_

    if active_connections_[0][2]:
        port = active_connections_[0][2][2]   # Get IP and Port of the local network
    else:
        port = None

    return port
# ---------------------------------------------------------
# Get the IP and port which is used by an external node to identify this node
# ---------------------------------------------------------
def get_external_ip_port():
    global active_connections_

    if active_connections_[0][1]:
        ip_port = active_connections_[0][1][0]   # Get IP and Port of the extenal network
    else:
        ip_port = None

    return ip_port
# ---------------------------------------------------------
# Get the IP which is used by an external node to identify this node
# ---------------------------------------------------------
def get_external_ip():
    global active_connections_

    if active_connections_[0][1]:
        ip = active_connections_[0][1][1]   # Get the IP of the local network
    else:
        ip = None

    return ip
# ---------------------------------------------------------
# Get the port which is used by an external node to identify this node
# ---------------------------------------------------------
def get_external_port():
    global active_connections_

    if active_connections_[0][1]:
        port = active_connections_[0][1][2]   # Get the Port of the external network
    else:
        port = None

    return port
# ---------------------------------------------------------
# Test if the IP is the external IP
# ---------------------------------------------------------
def is_external_ip(ip):
    global active_connections_

    if active_connections_[0][1]:
        # Test the IP and Port of the TCP external connection
        ret_val = (active_connections_[0][1][1] == ip)
    else:
        ret_val = False

    return ret_val

# ---------------------------------------------------------
# Test if the IP and port is the external IP port
# ---------------------------------------------------------
def is_external_ip_port(ip, port):
    global active_connections_

    if active_connections_[0][1]:
        # Test the IP and Port of the TCP external connection
        ret_val = (active_connections_[0][1][1] == ip and active_connections_[0][1][2] == port)
    else:
        ret_val = False

    return ret_val

# ---------------------------------------------------------
# Reply with the IP and Port from the active connections
# Server Type:  0 --> TCP, 1 --> REST
# connection:   1 --> External, 2 --> local
# ---------------------------------------------------------
def get_ip_port(server_type,connection_type):
    global active_connections_
    if active_connections_[server_type][connection_type]:
        ip_port = active_connections_[server_type][connection_type][0]
    else:
        ip_port = ""
    return ip_port
# ---------------------------------------------------------
# Get the Local or Global IP for TCP or rest
# ---------------------------------------------------------
def get_source_addr(dest_type, dest_ip):
    '''
    dest_type - TCP or rest
    dest_ip - the global address of the destination
    '''
    global active_connections_

    if dest_type == "rest":
        server_type = 1
    else:
        server_type = 0     # TCP


    if active_connections_[server_type][1]:
        ip_port = active_connections_[server_type][1][0]
        if dest_ip and ip_port.startswith(dest_ip + ":"):
            ip_port = active_connections_[server_type][2][0]
    else:
        ip_port = ""

    return ip_port

# ---------------------------------------------------------
# get the IP  active connections
# ---------------------------------------------------------
def get_connection_ip(server_type,connection_type):

    global active_connections_
    ip_port = active_connections_[server_type][connection_type]
    index = ip_port.find(':')
    if index > 0:
        ip = ip_port[:index]
    else:
        ip = ""
    return ip

# ----------------------------------------------------------------
# Set an IP and port on the message header - used to identyfy the destination
# for the reply message.
# ip addr set to 0.0.0.0 will take the reply address from the socket
# ----------------------------------------------------------------
def set_reply_ip(ip_addr, ip_port):

    global reply_addr_
    global reply_port_

    reply_addr_ = ip_addr
    reply_port_ = ip_port

# ----------------------------------------------------------------
# Set an IP and port that is used when a node sends a message to itself
# ----------------------------------------------------------------
def set_self_ip(ip_addr, ip_port):
    global self_addr_  # An IP to use when the node sends a message to itself
    global self_port_

    self_addr_ = ip_addr
    self_port_ = ip_port

# ----------------------------------------------------------------
# Test if the nodes is sending a message to itself - use the assigned IP and Port
# This is used in Kubernetes
# ----------------------------------------------------------------
def is_use_self(host, port):
    global self_addr_  # An IP to use when the node sends a message to itself
    global self_port_

    return self_addr_ and active_connections_[0][2][1] == host and active_connections_[0][2][2] == port # Get IP and Port of the local network

# ----------------------------------------------------------------
# Return the IP and Port defined using: set self ip and port = dynamic:7848
# ----------------------------------------------------------------
def get_self_ip_port():
    global self_addr_  # An IP to use when the node sends a message to itself
    global self_port_
    return [self_addr_, self_port_]
# ----------------------------------------------------------------
# Return the external IP
# ----------------------------------------------------------------
def get_reply_ip(status, io_buff_in, cmd_words, trace):

    reply = get_ip_port(0,2)
    return [process_status.SUCCESS, reply]

# ----------------------------------------------------------------
# Return the external IP
# ----------------------------------------------------------------
def get_ip():

    ip_port = get_ip_port(0,1)
    index = ip_port.find(":")
    if index > 0:
        ip = ip_port[:index]
    else:
        ip = ""
    return ip
# ----------------------------------------------------------------
# Return the external port
# ----------------------------------------------------------------
def get_port():

    ip_port = get_ip_port(0,1)
    index = ip_port.find(":")
    if index > 0 and index < (len(ip_port) - 2):
        port = ip_port[index + 1:]
    else:
        port = ""
    return port

# ----------------------------------------------------------------
# Test the host and port
# ----------------------------------------------------------------
def test_host_port(ip_port):
    index = ip_port.rfind(':')
    if index <= 0 or index == (len(ip_port) - 1):
        reply = "Host and port '%s' can not be identified" % ip_port
        ret_val = process_status.ERR_dest_IP_Ports
    else:
        host = ip_port[:index]
        port = ip_port[index + 1:]
        if port.isnumeric():
            port_number = int(port)
            try:
                target_ip = socket.gethostbyname(host)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                conn = s.connect_ex((target_ip, port_number))
            except:
                errno, value = sys.exc_info()[:2]
                reply = "Host and port '%s' are not accessible - error %s:%s" % (ip_port, str(errno), str(value))
                ret_val = process_status.ERR_dest_IP_Ports
            else:
                if not conn:
                    reply = "Port '%s' on host %s is open" % (port, host)
                    s.close()
                else:
                    if conn == 10061:
                        reply = "Not able to connect to '%s' with error #%u (connection refused)" % (ip_port, conn)
                    else:
                        reply = "Not able to connect to '%s' with error #%u" % (ip_port, conn)
                ret_val = process_status.SUCCESS
        else:
            reply = "Port is not numeric: %s" % port
            ret_val = process_status.ERR_dest_IP_Ports

    return [ret_val, reply]

# -------------------------------------------------------------
# Note: IP addresses start with 10,172 and 192 are used in Local networks
'''
Start Address    End Address   Number of Individual
                                    IP Addresses
192.168.0.0     192.168.255.255     65,536
172.16.0.0      172.31.255.255      1,048,576
10.0.0.0        10.255.255.255      16,777,216
127.0.0.0       127.255.255.255

And Adding: “Carrier Grade NAT” (CGNAT) address space - shared network on top of the regular Internet
https://tailscale.com/kb/1015/100.x-addresses/
100.64.0.0      100.127.255.255
'''
# -------------------------------------------------------------
def is_local_network(ip):
    ret_val = False
    if len(ip) >= 8:
        if ip[:3] == "10." or ip[:8] == "192.168." or ip[:4] == "127.":
            ret_val = True
        elif ip[:4] == "172." and ip[6] == '.':
            try:
                value = int(ip[4:6])
            except:
                pass
            else:
                if value >= 16 and value <= 31:
                    ret_val = True
        elif ip[:4] == "100.":
            # “Carrier Grade NAT” (CGNAT) address space
            # 100.64.0.0   to   100.127.255.255
            index = ip[4:].find('.')
            if index >=2:
                try:
                    value = int(ip[4:4+index])
                except:
                    pass
                else:
                    if value >= 64 and value <= 127:
                        ret_val = True

    return ret_val


# -------------------------------------------------------------
# Update the connection
# -------------------------------------------------------------
def add_connection(connect_id, url_external, port_external, url_internal, port_internal, url_bind, port_bind):

    global active_connections_

    if not isinstance(url_external, str) and not isinstance(url_internal, str):
        return process_status.ERR_source_IP_Ports
    if not isinstance(port_external, int) and not isinstance(port_internal, int):
        return process_status.ERR_source_IP_Ports

    ret_val = process_status.SUCCESS

    # add nex connection
    if url_external:
        connections_key_external = url_external + ":" + str(port_external)

        if not is_valid_ip_port(connections_key_external):
            utils_print.output_box("Warning: The IP and port are not valid: '%s'" % connections_key_external)
    else:
        connections_key_external = f"0.0.0.0:{port_bind}"

    if url_internal and port_internal:
        connections_key_internal = url_internal + ":" + str(port_internal)
        if not is_valid_ip_port(connections_key_internal):
            utils_print.output_box("Warning: The IP and port are not valid: '%s'" % connections_key_internal)
    else:
        connections_key_internal = connections_key_external

    if is_connection_used(connections_key_external):
        process_log.add_and_print("Error", "Host and port '%s' already in use" % connections_key_external)
        ret_val = process_status.Connection_error
    elif connections_key_internal != connections_key_external:
        if is_connection_used(connections_key_internal):
            process_log.add_and_print("Error", "Host and port '%s' already in use" % connections_key_internal)
            ret_val = process_status.Connection_error

    if not url_bind:
        # Listening to all IPs
        connections_key_bind = f"0.0.0.0:{port_bind}"
    else:
        # Listening to one IP
        connections_key_bind = f"{url_bind}:{port_bind}"
        url_external = url_bind
        port_external = port_bind
        url_internal = url_bind
        port_internal = port_bind

    if not ret_val:
        # maintain 3 fields:                    ip:port                   ip            port (int)
        active_connections_[connect_id][1] = (connections_key_external, url_external, port_external)
        active_connections_[connect_id][2] = (connections_key_internal, url_internal, port_internal)
        active_connections_[connect_id][3] = (connections_key_bind, url_bind, port_bind)

    return ret_val

# -------------------------------------------------------------
# Remove connection
# -------------------------------------------------------------
def remove_connection(connect_id):
    global active_connections_

    active_connections_[connect_id][1] = None
    active_connections_[connect_id][2] = None
    active_connections_[connect_id][3] = None   # Bind

# -------------------------------------------------------------
# Test if IP + Port are being used by another process
# -------------------------------------------------------------
def is_connection_used(ip_port):
    global active_connections_

    for connection_info in active_connections_:
        for conect_used in connection_info[1:]:
            if conect_used:
                if ip_port in conect_used:        # entry is an array of 2 connections: internal external
                    return True
    return False

# -------------------------------------------------------------
# Test if the connection is active:
# 0 - Network
# 1 - REST
# -------------------------------------------------------------
def is_active_connection(connect_id):
    global active_connections_
    return active_connections_[connect_id][1] != None

def is_tcp_connected():
    return is_active_connection(0)
def is_rest_connected():
    return is_active_connection(1)
def is_msg_connected():
    return is_active_connection(2)


# -------------------------------------------------------------
# Test if the connection is active:
# 0 - Network
# 1 - REST
# -------------------------------------------------------------
def get_connection_info(connect_id):
    global active_connections_

    # Return the external + local (if different) IP and Port
    if active_connections_[connect_id][1]:
        connection = "Listening on: "
        connection += active_connections_[connect_id][1][0]
        if active_connections_[connect_id][2][0] != active_connections_[connect_id][1][0]:
            connection += " and " + active_connections_[connect_id][2][0]
    else:
        connection = "No active connection"

    return connection

# ------------------------------------------
# Get The OUTSIDE connections
# Command: get connections
# Returns ret_code + the IP and ports of the TCP and REST connections
# ------------------------------------------
def get_connections(status, io_buff_in, cmd_words, trace):

    global active_connections_
    global reply_addr_
    global reply_port_
    global self_addr_
    global self_port_

    if cmd_words and cmd_words[-1] == "json":
        # get connections where format = "json"
        connection_dict = {}
        json_format = True
        out_format = "json"
    else:
        connections_list = []
        json_format = False
        out_format = "table"

    for entry in active_connections_:
        if entry[1]:
            external_addr = entry[1][0]
            local_addr = entry[2][0]
            bind_addr = entry[3][0]
        else:
            external_addr = "Not declared"
            local_addr = "Not declared"
            bind_addr = "Not declared"
        if json_format:
            connection_dict[entry[0]] = {
                "external" : external_addr,
                "local": local_addr,
                "bind": bind_addr,
            }
        else:
            connections_list.append((entry[0],external_addr, local_addr, bind_addr))

    if reply_addr_:
        ip_port = f"{reply_addr_}:{reply_port_}"
        if json_format:
            connection_dict["reply"] = ip_port
        else:
            connections_list.append(("Reply Address", ip_port , "", ""))

    if self_addr_:
        ip_port = f"{self_addr_}:{self_port_}"
        if json_format:
            connection_dict["self"] = ip_port
        else:
            connections_list.append(("Self Address", "", ip_port, ""))

    if json_format:
        reply = utils_json.to_string(connection_dict)
    else:
        reply = utils_print.output_nested_lists(connections_list, None, ["Type", "External Address", "Internal Address", "Bind Address"], True, "")
    return [process_status.SUCCESS, reply, out_format]
