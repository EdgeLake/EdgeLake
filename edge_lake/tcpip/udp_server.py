"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


#
# import socket programming library
import socket
import sys
import os
import struct
import hashlib

# import thread module
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.params as params
import edge_lake.generic.process_log as process_log
import edge_lake.generic.process_status as process_status
import edge_lake.tcpip.message_header as message_header
import edge_lake.cmd.member_cmd as member_cmd

blocks_processed = {}


def udp_server(params: params, host: str, port: int, buff_size: int):
    """
    :param host: IP Address
    :param port: unprivleged users need to bind to pot 1024 or higher
    :return:
    """
    memcmd = member_cmd.MemberCmd()

    data_buffer = bytearray(buff_size)
    mem_view = memoryview(data_buffer)

    status = process_status.ProcessStat()

    try:
        soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except:
        error_msg = "UDP server: failed to create a socket - " + str(sys.exc_info())
        process_log.add_and_print("Error", error_msg)
        sys.exit()

    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # SO_REUSEADDR - ignoe OS timeout for the socket
    # socket created

    try:
        soc.bind((host, port))
    except:
        error_msg = "UDP server: Bind failed error - " + str(sys.exc_info())
        process_log.add_and_print("Error", error_msg)
        sys.exit()

    #   s.setblocking(False)

    process_log.add("Event", "UDP server initiated on %s:%u" % (host, port))

    # a forever loop until client wants to exit

    soc.settimeout(3)

    while True:

        try:
            # establish connection with client
            mem_view[0:], client_addr = soc.recvfrom(buff_size)

        except:
            if process_status.is_exit("tcp"):
                break
            errno, value = sys.exc_info()[:2]
            continue

        # calculate the hash value and test that the hash received is the same as the hash calculated
        message_size = message_header.get_block_size_used(mem_view)
        hash_value = hashlib.md5(
            mem_view[message_header.HASH_LENGTH:message_size]).digest()  # returns 128 bits (16 bytes) hash value

        try:
            soc.sendto(hash_value, client_addr)  # return the hash value calculated
        except:
            # might be that the node does not see an error and will not resend the block - if the hash is correct - do command
            errno, value = sys.exc_info()[:2]  # CONTINUE - if the node has an error, it will resend the block

        if hash_value == message_header.get_hash_value(mem_view):
            # block was received properly

            process_data(memcmd, status, client_addr[0], str(client_addr[1]), mem_view)

        if process_status.is_exit("tcp"):
            break

    process_log.add("Error", "Error - Terminating UDP Server")

    soc.close()


# ------------------------------------------------------------------------
# Process the data in the block received
# ------------------------------------------------------------------------
def process_data(memcmd, status, ip_in, port_in, mem_view: memoryview):
    # make sure that this is the first occurence of this block
    socket_id, time_id, send_counter = message_header.get_send_socket_info(
        mem_view)  # get the info on the sender socket

    message_time = str(struct.unpack("d", time_id)[0])
    job_key = ip_in + "." + str(socket_id)  # an ID identifying the job instance on the sender node

    # Test if a socket is being reused or a node restarts - the value is set to 1 - a
    if job_key in blocks_processed:
        previous_key = blocks_processed[job_key]
        offset = previous_key.find(':')
        previous_time = previous_key[:offset]
        previous_counter = previous_key[offset + 1:]
        if previous_time == message_time and send_counter <= int(previous_counter):
            # test this is a newer block than the last block (of the job) processed
            return 0  # old block -> ignored

    blocks_processed[job_key] = (str(message_time) + ':' + str(send_counter))  # replace the value

    # get as many words that are needed to generate a command
    command = memcmd.get_executable_command(status, None, mem_view)

    command_ret_val = -1
    if command != "":
        command += " message"  # this would trigger __process_job() to process the job reply
        status.reset()
        command_ret_val = memcmd.process_cmd(status, command=command, print_cmd=False, source_ip=ip_in,
                                             source_port=port_in, io_buffer_in=mem_view)

    return command_ret_val


def get_ip():
    """
    Get IP address of given node
    :return:
       IP Address
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


def get_ip_addresses(ifname):
    """
    Get the IP address from the OS by calling 'ip addr'
    :param ifname: name string to search
    :return: ip address which is not '127.0.0.1'
    """

    ip_text = os.popen('ip addr').read()  # find 'inet' or 'inet6' or'eth0'

    name_length = len(ifname)
    ip_addr = ""
    index = 0

    while 1:
        index = ip_text.find(ifname, index)
        if index == -1:
            break
        index += name_length
        ip_addr = utils_data.get_ip_addr(ip_text[index:])
        if ip_addr == "":
            continue
        if (ip_addr != "127.0.0.1"):
            break
        index += len(ip_addr)
    return ip_addr
