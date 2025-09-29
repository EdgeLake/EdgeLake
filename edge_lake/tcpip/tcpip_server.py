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
from requests import get

# import thread module

import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.params as params
import edge_lake.generic.process_log as process_log
import edge_lake.generic.process_status as process_status
import edge_lake.tcpip.message_header as message_header
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.version as version
import edge_lake.generic.trace_methods as trace_methods

class PeerClosed(Exception): pass       # clean EOF (no data read)
class PartialFrame(Exception): pass     # closed mid-frame (some data read)

# Use MSG_WAITALL on Linux; Windows can be inconsistent → still loop.
_USE_WAITALL = hasattr(socket, "MSG_WAITALL") and not sys.platform.startswith("win")

workers_pool = None

configured_nic_ip_ = ""     # User can set the NIC ID using the command: set internal ip with enp0s3

def reset_nic_ip():
    global configured_nic_ip_
    configured_nic_ip_ = ""

# ----------------------------------------------------------------
# Init TCP server
# ----------------------------------------------------------------
def tcp_server( host: str, port: int, is_bind:bool, workers_count, trace):
    global workers_pool


    # Set a pool of workers threads
    workers_pool = utils_threads.WorkersPool("TCP", workers_count)
    buffer_size = params.TCP_BUFFER_SIZE
    params.add_param("anylog_server_port", str(port))

    net_utils.message_server("TCP Server", "tcp", host, port, buffer_size, workers_pool, rceive_data, is_bind, trace)

    net_utils.remove_connection(0)
    workers_pool = None

# ----------------------------------------------------------------
# Receive data and process
# ----------------------------------------------------------------
def rceive_data(status, mem_view, params, clientSoc, ip_in, port_in, max_buffr_size):
    clientSoc.setblocking(1)  # wait for the data to be received (equal to soc.settimeout(None)
    # clientSoc.settimeout(5)

    command_ret_val = 0
    ret_val = process_status.SUCCESS
    recv_counter = 0  # for debug - number of messages that are retrieved
    unique_job_id = 0

    trace_level = member_cmd.commands["run tcp server"]['trace']

    if trace_level > 2:
        utils_print.output("[TCP Server receiving data on socket] %s" % str(clientSoc),  True)

    if net_utils.get_TCP_debug() or trace_level >= 5:
        utils_print.output_box("TCP Server receiving data using: %s" % str(clientSoc))


    while True:

        trace_network = trace_methods.is_traced("tcp in")
        if trace_network:
            # Enabled by: -  trace method on tcp out
            details = {"Method": "Receive Data"}
            trace_methods.add_details("tcp in", **details)

        n = 0
        try:
            n = _recv_exact_into(clientSoc, mem_view, max_buffr_size, timeout=None)
            # process mem_view[:n] ...

        except socket.timeout as e:
            # keep waiting on benign timeouts
            continue

        except PeerClosed as e:
            # Clean disconnect (no data). Break quietly.
            process_log.add("Info", f"Peer closed {clientSoc.getpeername()} before data")
            break

        except PartialFrame as e:
            # Error: closed mid-frame. Log and break.
            n = getattr(e, "bytes_read", 0)
            payload_view = mem_view[:n] if n else None
            show_error(clientSoc, e, type(e), ip_in, port_in, trace_network,
                       payload=payload_view, mem_view=mem_view)
            # optional: preview
            # process_log.add("Error", f"partial frame {n}/{max_buffr_size} | {_preview_view(mem_view, n)}")
            ret_val = process_status.ERR_network
            if unique_job_id:
                member_cmd.stop_job_signal_rest(status, ret_val, job_location, unique_job_id)
            break

        except Exception as e:
            # Generic failure
            n = getattr(e, "bytes_read", 0)
            payload_view = mem_view[:n] if n else None
            show_error(clientSoc, e, type(e), ip_in, port_in, trace_network,
                       payload=payload_view, mem_view=mem_view)
            ret_val = process_status.ERR_network
            if unique_job_id:
                member_cmd.stop_job_signal_rest(status, ret_val, job_location, unique_job_id)
            break

        if not message_header.is_source_ip_included(mem_view):
            # if the message does not have a source IP, add source IP from the tcp connection
            try:
                peer_ip = clientSoc.getpeername()[0]  # The IP of the peer node
            except:
                peer_ip = None
            else:
                message_header.add_ip(mem_view, peer_ip)

        recv_counter += 1

        # debug_message(mem_view, recv_counter, ip_in, port_in)

        if message_header.get_block_id(mem_view) != recv_counter:
            err_msg = "TCP server status is in inconsistent with message: incoming messages: %u, message header: %u" \
                      % (recv_counter, message_header.get_block_id(mem_view))
            process_log.add("Error", err_msg)
            utils_print.output(err_msg, True)
            ret_val = process_status.ERR_network
            if unique_job_id:
                # signal the REST thread that there is an error
                if message_header.is_source_ip_port(mem_view, net_utils.get_external_ip_port()):  # test if this is the server initiating the message
                    member_cmd.stop_job_signal_rest(status, ret_val, job_location, unique_job_id)

            if trace_network:
                details = {
                    "Error": err_msg,
                }
                trace_methods.add_details("tcp in", **details)
                trace_methods.print_details("tcp in", "red")

            break

        is_last_block = message_header.is_last_block(mem_view)  # need to be done before block is modified for send data

        # get as many words that are needed to generate a command
        command = member_cmd.get_executable_command(status, None, mem_view)

        if trace_network:
            # Enable by: trace method on tcp in
            # View status: get trace info where process = "tcp in"
            if recv_counter == 1:
                # First time
                if command:
                    cmd_text = message_header.get_command(mem_view)  # provide all details of command
                else:
                    cmd_text = "Not a valid command"

                details = {
                    "Command": cmd_text,
                    "Source Node": f"{ip_in}:{port_in}",
                    "Counter" : 1,
                    "Status": "Prep",
                }
                trace_methods.add_details("tcp in", **details)
            else:

                details = {
                    "Counter" : recv_counter,
                }
                trace_methods.update_details("tcp in", **details)

        if recv_counter == 1:
            # first block in a sequence of blocks
            pub_key = None
            if version.al_auth_is_node_authentication() and command != "job":
                # with SQL, process is done when dbms name and table name are resolved at - _issue_sql
                # "job" is a reply to a request and is always allowed

                ret_val, pub_key = version.permissions_authenticate_tcp_message(status, mem_view)
                if ret_val:
                    # Authentication error
                    if trace_network:
                        details = {
                            "Error": "Authentication Failed",
                        }
                        trace_methods.add_details("tcp in", **details)
                        trace_methods.print_details("tcp in", "red")
                    break


            if net_utils.is_source_node(mem_view):
                job_location = message_header.get_job_location(mem_view)
                unique_job_id = message_header.get_job_id(mem_view)  # a unique ID of this JOB
            else:
                unique_job_id = 0       # This is not a reply with a job instance


        if command == "sql" and member_cmd.is_debug_method("query"):
            # Debug process to follow on data returned for queries
            member_cmd.debug_to_job_instance(mem_view)

        if command != "":
            command += " message"  # this would trigger __process_job() to process the job reply

            if not command_ret_val or (unique_job_id and member_cmd.is_with_subset(status, job_location, unique_job_id)):
                # Either:
                # a) No error
                # b) Process with partial results (subset flag is set to true)

                status.reset(pub_key)

                command_ret_val = member_cmd.process_cmd(status, command=command, print_cmd=False, source_ip=ip_in,
                                                         source_port=port_in, io_buffer_in=mem_view)
                if command_ret_val and command_ret_val != process_status.Empty_data_set:

                    # signal the REST thread that there is an error
                    if unique_job_id:  # Source node is True if the message is a reply
                        # :  # test if this is the server initiating the message
                        if message_header.is_source_ip_port(mem_view, net_utils.get_external_ip_port()):
                            member_cmd.stop_job_signal_rest(status, command_ret_val, job_location, unique_job_id)


        else:
            # return error message - unrecogbized command
            err_msg = "echo Command '%s' not recoginzed" % message_header.get_command(mem_view)
            member_cmd.error_message(status, mem_view, process_status.ERR_unrecognized_command,
                                     message_header.BLOCK_INFO_COMMAND, err_msg, "")

        if trace_network:
            details = {
                "Status": "Success",
            }
            trace_methods.update_details("tcp in", **details)

        if is_last_block or message_header.get_error(mem_view):
            # debug_message(mem_view, recv_counter, ip_in, port_in)
            break

    if trace_level >= 5:
        utils_print.output_box("TCP Server thread completed process using: %s" % str(clientSoc))

    if ret_val and member_cmd.is_debug_method("query"):
        utils_print.output_box("TCP Server: Thread processing message failed with error: %s" % process_status.get_status_text(ret_val))


def _preview_view(mv, n: int, max_len: int = 96) -> str:
    """Tiny preview of first n bytes in mv (ASCII + hex). Copies at most max_len bytes."""
    n = min(n, len(mv))
    shown = mv[:min(n, max_len)].tobytes()   # small, bounded copy for logs
    ascii_part = ''.join(chr(x) if 32 <= x < 127 else '.' for x in shown)
    hex_part = ' '.join(f'{x:02x}' for x in shown)
    suffix = " ..." if n > max_len else ""
    return f'len={n} ascii="{ascii_part}" hex={hex_part}{suffix}'

def _recv_exact_into(sock: socket.socket, mv, frame_len: int, timeout: float | None = None) -> int:
    """
    Read exactly frame_len bytes directly into memoryview 'mv' (no temporary bytes).
    Returns the total bytes read (== frame_len) on success.
    On any exception, attaches 'bytes_read' to the exception (partial count) and re-raises.
    """
    if timeout is not None:
        sock.settimeout(timeout)

    total = 0
    flags = socket.MSG_WAITALL if (hasattr(socket, "MSG_WAITALL") and not sys.platform.startswith("win")) else 0
    try:
        while total < frame_len:
            r = sock.recv_into(mv[total:frame_len], frame_len - total, flags)
            if r == 0:
                exc = PeerClosed("peer closed before any data") if total == 0 \
                      else PartialFrame(f"peer closed mid-frame ({total}/{frame_len})")
                exc.bytes_read = total
                raise exc
            total += r
        return total
    except Exception as e:
        # e.bytes_read = total: adds a custom attribute to the caught exception instance so the caller can later do getattr(e, "bytes_read", 0) and know how many bytes were read before the failure.
        try: e.bytes_read = total
        except: pass
        raise
# ------------------------------------------------------------------
# Return the workers pool
# returns info when calling - get tcp pool
# ------------------------------------------------------------------
def get_threads_obj():
    global workers_pool
    return workers_pool

# ------------------------------------------------------------------
# Return info on the TCP Server in command - show processes
# ------------------------------------------------------------------
def get_info( status = None ):
    global workers_pool

    info_str = net_utils.get_connection_info(0)
    if workers_pool:
        info_str += ", Threads Pool: %u" % workers_pool.get_number_of_threds()

    return info_str

# ------------------------------------------------------------------
# Return info on the TCP Server in command - show processes
# ------------------------------------------------------------------
def get_workers_pool():
    global workers_pool
    return workers_pool

# ------------------------------------------------------------------
# Set the IP method to use when the IP string is retrieved
# ------------------------------------------------------------------
def set_local_ip_method(status, interface_name):
    global configured_nic_ip_
    ret_val, nic_ip = net_utils.get_ip_by_nic(status, interface_name)
    if not ret_val:
        configured_nic_ip_ = nic_ip

    return [ret_val, nic_ip]


def get_ip():
    """
    Get IP address of given node
    :return:
       IP Address
    """
    global configured_nic_ip_

    if configured_nic_ip_:
        ip = configured_nic_ip_       # User configured to the IP
    else:

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            s.connect(("8.8.8.8", 80))
            ip = str(s.getsockname()[0])
        except:
            ip = "127.0.0.1"  # no internet connection

    return ip


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

def get_external_ip():

    for i in range (2):     # Try twice if failure
        try:
            external_ip = get('https://api.ipify.org', timeout=3).text      # waits at most 3 seconds to get external ipb
        except:
            external_ip = get_ip()    # no internet connection or  get_ip_addresses(ifname)
        else:
            if not net_utils.test_ipv4(external_ip, "100"):
                external_ip = get_ip()    # no internet connection or  get_ip_addresses(ifname)
            else:
                break       # No failure

    return external_ip

# ---------------------------------------------------------
# Debug incoming messages, including THREAD ID on the PEER NODE
# ---------------------------------------------------------
def debug_message(mem_view, recv_counter, ip_in, port_in):
    file_no, time_val, thread_id = message_header.get_send_socket_info(mem_view)
    text = "\nis_last = %u, recev_counter = %u, block_id = %u, par_id = %u %s:%s, file_no = %u, thread_id = %u" % (
    message_header.is_last_block(mem_view), recv_counter, message_header.get_block_id(mem_view),
    message_header.get_partition_id(mem_view), ip_in, port_in, file_no, thread_id)
    utils_print.output(text, False)

# ---------------------------------------------------------
# Print the error message
# ---------------------------------------------------------
# ---------------------------------------------------------
# Print the error message (enhanced; extra args are optional)
# ---------------------------------------------------------
def show_error(connection, value, errno, ip_in, port_in, trace_network, *, payload=None, mem_view=None, max_preview=96):
    """
    Log a socket/server error.
    - connection: socket object (may be closed)
    - value:      exception instance
    - errno:      exception type/class (as per your current usage)
    - ip_in/port_in: peer info already tracked by caller
    - trace_network: if True, also emit to trace_methods
    - payload:    optional bytes/bytearray/memoryview received before failure
    - mem_view:   optional memoryview target buffer (structure logged)
    - max_preview: max bytes to preview from payload
    """
    # Header (kept from original)
    err_msg = "TCP server received unrecognized message or client (%s:%s) disconnected" % (ip_in, port_in)

    # Socket repr (safe, even if closed)
    err_msg += "\nConnection:  %s" % str(connection)

    # BROKEN PIPE guard
    try:
        is_broken_pipe = (len(getattr(value, "args", ())) > 0 and value.args[0] == net_utils.BROKEN_PIPE)
    except Exception:
        is_broken_pipe = False

    if is_broken_pipe:
        err_msg += "\nError:      BROKEN PIPE"

    # Core exception line (kept)
    err_msg += "\nError:      %s : %s" % (str(errno), str(value))

    # Optional deep context (only if provided by caller)
    if payload is not None:
        err_msg += "\nPayload:    " + _preview_bytes(payload, max_len=max_preview)
    if mem_view is not None:
        err_msg += "\nMemView:    " + _memview_info(mem_view)

    # Summary line (kept)
    if is_broken_pipe:
        err_info = "TCP server disconnected from client: BROKEN PIPE"
    else:
        err_info = "TCP server disconnected from client({0}:{1}) Error: {2} : {3}".format(
            ip_in, port_in, str(errno), str(value)
        )

    # Trace or print (kept behavior)
    if trace_network:
        details = {"Error": err_msg, "Error Info": err_info}
        trace_methods.add_details("tcp in", **details)
        trace_methods.print_details("tcp in", "red")
    else:
        utils_print.output_box(err_msg + "\r\n" + err_info)

    process_log.add("Error", err_info)

# ---------------------------------------------------------
# Helpers for optional deep context (used only if provided)
# ---------------------------------------------------------
def _preview_bytes(buf, max_len: int = 96) -> str:
    """
    Compact dump of payload: total length + ASCII + hex preview.
    Accepts bytes, bytearray, memoryview; copies at most max_len bytes.
    """
    if buf is None:
        return "None"
    mv = memoryview(buf)
    n = min(len(mv), max_len)
    shown = mv[:n].tobytes()
    ascii_part = ''.join(chr(x) if 32 <= x < 127 else '.' for x in shown)
    # hex_part   = ' '.join(f'{x:02x}' for x in shown)
    # suffix = " ..." if len(mv) > max_len else ""
    # return f'len={len(mv)} ascii="{ascii_part}" hex={hex_part}{suffix}'
    return f"Data received in socket: {ascii_part}"

def _memview_info(mv) -> str:
    """
    Describe a memoryview’s structure for debugging assignment errors.
    """
    if mv is None:
        return "None"
    try:
        return (f"nbytes={mv.nbytes} format={getattr(mv,'format',None)} "
                f"itemsize={getattr(mv,'itemsize',None)} ndim={getattr(mv,'ndim',None)} "
                f"shape={getattr(mv,'shape',None)}")
    except Exception as e:
        return f"<memview info error: {e}>"
