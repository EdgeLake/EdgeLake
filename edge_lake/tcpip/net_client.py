"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# Echo client program
import socket
import sys
import threading
import time
import errno
import select

# import edge_lake.generic.utils_io as util_io
import edge_lake.tcpip.message_header as message_header
import edge_lake.generic.process_log as process_log
import edge_lake.generic.params as params
import edge_lake.generic.utils_print as utils_print
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.process_status as process_status
import edge_lake.generic.version as version
import edge_lake.generic.trace_methods as trace_methods

# run tcp client 10.0.0.124 2048 read aaa bbb

# =======================================================================================================================
# Change mode to UDP - this is called if UDP server is initiates
# =======================================================================================================================
def use_udp():
    global USE_UDP
    USE_UDP = True

# =======================================================================================================================
# Send large message to a file on a destination node
# The message is send as a file that will be read and processed by the peer
# =======================================================================================================================
def large_message(status, ip:str, port:int, mem_view, large_msg, data):

    source_ip = net_utils.get_external_ip()  # Serves to identify the node on the file name
    if not source_ip:
        source_ip = net_utils.get_local_ip()

    output_file = f"cmd.{source_ip}.{utils_threads.get_thread_number()}.lmsg"

    ret_val, auth_str = version.al_auth_get_transfer_str(status, net_utils.get_external_ip_port())
    if ret_val:
        return ret_val

    if not file_send(status, ip, port, None, large_msg, output_file, message_header.LARGE_MSG, 0, False, auth_str):
        status.add_error(f"Failed to transfer Large Command to dest node at: {ip}:{port}")
        ret_val = process_status.NETWORK_CONNECTION_FAILED
    return ret_val

# =======================================================================================================================
# Loop while data is read from file and send to dest server
# messages send include header + data
# =======================================================================================================================
def file_send(status, host: str, port: int, input_file: str, large_msg: str, output_file: str, flag: int, trace: int, consider_err:bool, auth_data:str):
    '''
    host - destination IP
    port - destination port
    input_file - the name of the input file
    large_msg - A message that does not fit to a single block - provide either input_file or large_msg
    output_file - the name of the output file
    input_msg - A message string that does not fit a single block
    flag - determines the behaviour on the destination node. Flags are defined in message_header.py: GENERIC_USE_WATCH_DIR, LEDGER_FILE, LARGE_MSG
    trace - the trace level
    consider_err - generate error message in case of a failure
    auth_data - signed IP:Port + Date-Time
    '''
    ret_val = True

    data_buffer = status.get_io_buff()

    buff_size = int(params.get_param("io_buff_size"))

    mem_view = memoryview(data_buffer)

    # set the destination IP and Port for outgoing messages
    if net_utils.set_ip_port_in_header(None, mem_view, host, port):
        return False

    message_header.set_generic_flag(mem_view, flag)  # this is a flag to the function called. For example, place file in watch dir

    output_file = output_file.replace(' ', '\t')  # Make a no space string when the message is read on the destination node

    if not output_file:
        # same file name as input file
        if flag:
            # flag determines where to place the file on dest machine - only extract file name
            file_name, file_type = utils_io.extract_name_type(input_file)
            if file_type:
                dest_file = file_name + '.' + file_type
            else:
                dest_file = file_name   # without flag - use the same name + path as the copied file
        else:
            dest_file = input_file
    else:
        dest_file = output_file


    offset_data = message_header.prep_command(mem_view, "file write " + dest_file)  # add the command size and info to the buffer

    # send a signed message with the IP, Port and Time that can be authenticated
    if not message_header.set_authentication(mem_view, auth_data):
        process_log.add("Error", "Internal Block Error - no space for authentication")
        return False

    if auth_data:
        # The first block includes the authentication string - the second data block can overwrite the string
        offset_in_buff = message_header.get_data_offset_after_authentication(mem_view)
    else:
        offset_in_buff = offset_data

    max_data_len = buff_size - offset_in_buff       # The max data that can it the block

    io_object = utils_io.IoHandle() if not large_msg else None # object maintaining file handle and status

    if host == net_utils.get_external_ip():
        use_ip = net_utils.get_local_ip()   # Use the local IP to connect
    else:
        use_ip = host


    soc = socket_open(use_ip, port, "file write", 6, 3)

    if (large_msg or utils_io.is_path_exists(input_file)):
        # Either a long message or a file on disk

        if soc:
            if large_msg or io_object.open_file("read", input_file):

                if trace > 1:
                    counter = 0
                    if trace == 2:
                        utils_print.output("\ncounter   read      copied", True)

                block_number = 0
                last_block = False
                if large_msg:
                    large_msg_offset = 0
                    command_encoded = large_msg.encode()
                    command_encoded_length = len(command_encoded)

                data_copied = 0
                while 1:  # loop while file is read and transferred

                    block_number += 1

                    if large_msg:
                        # Copy message to buffer the large message content in chanks
                        not_copied_length = command_encoded_length - data_copied
                        data_in = max_data_len if not_copied_length > max_data_len else not_copied_length
                        mem_view[offset_in_buff:offset_in_buff + data_in] = command_encoded[large_msg_offset:large_msg_offset + data_in]
                        large_msg_offset += data_in
                    else:
                        # read file to buffer
                        data_in = io_object.read_into_buffer(mem_view[offset_in_buff:])

                    data_copied += data_in
                    if trace > 1:
                        counter += 1
                        if large_msg:
                            print_info = "\r\n-->[Large Message Send] [Block #%u] [Bytes Copied: %u]" % (counter, data_copied)
                        else:
                            print_info = "\r\n-->[File Send] [Block #%u] [Bytes Copied: %u] [%s]" % (counter, data_copied, input_file)
                        utils_print.output(print_info, True)


                    message_header.incr_data_segment_size(mem_view, data_in)  # add length of data to message

                    if ((offset_in_buff + data_in) < buff_size):  # last message was send
                        last_block = True

                    message_header.set_block_number(mem_view, block_number,
                                                    last_block)  # place in the message header the block number and a flag representing last block to send

                    # if message_send(soc, data_buffer) == False:
                    if mem_view_send(soc, mem_view) == False:
                        break  # error sending the data

                    if last_block:
                        break

                    offset_in_buff = message_header.set_data_segment_to_command(mem_view)  # reset the size of the data in the block

                if input_file:
                    io_object.close_file()
            else:
                ret_val = False
        else:
            ret_val = False  # socket open failed
            error_msg = f"[File Copy Error] [Failed to open target socket: {use_ip}:{port}]"
            process_log.add("Error", error_msg)

    else:  # input file does not exists
        ret_val = False
        error_msg = "Input file does not exists: " + input_file
        process_log.add("Error", error_msg)

    if soc:
        socket_close(soc)

    if not ret_val:
        if consider_err:
            member_cmd.output_echo_or_stdout(None, None, "File send failure from local node to: %s:%s" % (host,port ))

    return ret_val


# =======================================================================================================================
# Open socket - Try X times before return an error
# =======================================================================================================================
import socket, errno, select, time, sys, os, struct, traceback

# --------------------------- Platform helpers --------------------------------

def _is_linux() -> bool:
    """Return True if running on Linux (for TCP_INFO support)."""
    return sys.platform.startswith("linux")

def _is_windows() -> bool:
    """Return True if running on Windows (for SIO_KEEPALIVE_VALS)."""
    return sys.platform.startswith("win")


# ---------------------------- Keepalive tuning --------------------------------
import sys, socket, struct

def _enable_keepalive(sock, idle: float | None = None, interval: float | None = None, count: int | None = None):
    """
    Enable TCP keepalive in a cross-platform way.
    - Windows: uses SIO_KEEPALIVE_VALS with a 3-tuple (onoff, time_ms, interval_ms).
    - Linux:   uses TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT when available.
    - macOS:   uses TCP_KEEPALIVE (idle), and TCP_KEEPINTVL / TCP_KEEPCNT if present.
    All failures are suppressed; logs 'Warn' and continues.
    """
    # Always try to enable keepalive bit first
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError as e:
        process_log.add("Warn", f"TCP Client: SO_KEEPALIVE enable failed: {e}")
        return

    try:

        if sys.platform.startswith("linux"):
            # Best-effort: options may not exist on all kernels
            if idle is not None and hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, int(idle))
            if interval is not None and hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(interval))
            if count is not None and hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(count))
        elif sys.platform.startswith("win"):
            # Windows expects a 3-item tuple, not bytes
            SIO_KEEPALIVE_VALS = getattr(socket, "SIO_KEEPALIVE_VALS", 0x98000004)
            onoff = 1
            time_ms = int((idle if idle is not None else 7200) * 1000)  # default 2h
            interval_ms = int((interval if interval is not None else 1) * 1000)  # default 1s
            sock.ioctl(SIO_KEEPALIVE_VALS, (onoff, time_ms, interval_ms))

        elif sys.platform == "darwin":  # macOS
            # macOS uses TCP_KEEPALIVE for idle time
            if idle is not None and hasattr(socket, "TCP_KEEPALIVE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, int(idle))
            if interval is not None and hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(interval))
            if count is not None and hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(count))
        else:
            # Other POSIX—try Linux-style constants if present
            if idle is not None and hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, int(idle))
            if interval is not None and hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(interval))
            if count is not None and hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(count))

    except OSError as e:
        process_log.add("Warn", f"TCP Client: keepalive tuning failed: {e}")


# --------------------------- Root-cause classifier ----------------------------

def _classify_connect_failure(exc=None, soerr: int | None = None, timed_out: bool = False) -> tuple[str, str]:
    """
    Return (reason, hint) strings for common connect failures across Linux/Windows.
    - exc: optional exception object
    - soerr: optional SO_ERROR integer from getsockopt
    - timed_out: True for connect timeouts (select() expiration)
    """
    e = getattr(exc, "errno", None)
    code = soerr if soerr is not None else e

    if timed_out:
        return ("Timed out (no SYN-ACK)",
                "Likely filtered by firewall/security group or the host is down. Verify listener and inbound rules.")

    if code in (getattr(errno, "ECONNREFUSED", 111), 10061):
        return ("Connection refused (RST)",
                "No service is listening on the target port or firewall actively rejects. Start the service or open the port.")

    if code in (getattr(errno, "ENETUNREACH", 101),):
        return ("Network unreachable",
                "Routing issue on the client. Check default route/VPC routing tables.")

    if code in (getattr(errno, "EHOSTUNREACH", 113),):
        return ("Host unreachable",
                "No route to host; verify remote IP/subnet/gateway.")

    if code in (getattr(errno, "ETIMEDOUT", 110),):
        return ("Connect timed out",
                "Packets dropped or remote overwhelmed; check path/firewalls/server load.")

    if code in (getattr(errno, "EADDRNOTAVAIL", 99),):
        return ("Local address not available",
                "Invalid source binding; check bind()/interface/IP assignment.")

    if isinstance(exc, socket.gaierror):
        return ("DNS resolution failed",
                "Host name not resolvable; check DNS/hosts and spelling.")

    if isinstance(exc, BlockingIOError) and e in (
        getattr(errno, "EINPROGRESS", 115),
        getattr(errno, "EALREADY", 114),
        getattr(errno, "EWOULDBLOCK", 11),
        10035,  # WSAEWOULDBLOCK on Windows
    ):
        return ("Connect in progress",
                "Non-blocking connect pending; wait for writability and then check SO_ERROR.")

    return (f"Socket error (code={code})", "Inspect server logs, firewall rules, and routing.")


# -------------------------- Optional Linux TCP_INFO ---------------------------

def _get_tcp_info(sock) -> dict | None:
    """
    Best-effort probe of Linux TCP_INFO to surface RTT/retransmits/state during failures.
    Returns a small dict or None on non-Linux or unsupported kernels.
    """
    if not _is_linux():
        return None
    TCP_INFO = getattr(socket, "TCP_INFO", None)
    if TCP_INFO is None:
        return None
    try:
        raw = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 192)
        fmt = "BBBBBBxx" + "I"*6  # compact, stable subset
        vals = struct.unpack_from(fmt, raw)
        return {
            "tcpi_state": vals[0],            # 1=ESTABLISHED, 3=SYN_SENT, etc.
            "tcpi_retransmits": vals[2],
            "tcpi_probes": vals[3],
            "tcpi_backoff": vals[4],
            "tcpi_options": vals[5],
            "tcpi_rto_ms": vals[6] // 1000,   # usec → ms
            "tcpi_rtt_ms": vals[7] // 1000,
            "tcpi_rttvar_ms": vals[8] // 1000,
            "tcpi_snd_ssthresh": vals[9],
            "tcpi_snd_cwnd": vals[10],
            "tcpi_advmss": vals[11],
        }
    except OSError:
        return None


# ----------------------------- Deep log helpers -------------------------------

def _log_deep_error(stage: str, host: str, port: int, sock, command: str, exc: Exception,
                    attempt: int, retry_counter: int) -> None:
    """
    Emit a detailed diagnostic for exception-based failures (final attempt only).
    Includes addresses, socket repr, errno, reason/hint, TCP_INFO (Linux), and traceback.
    """
    sock_info = str(sock) if sock else "Socket not created"
    local_addr = None
    try:
        if sock:
            local_addr = sock.getsockname()
    except OSError:
        pass

    reason, hint = _classify_connect_failure(exc=exc)
    tcp_info = _get_tcp_info(sock)
    # tb = traceback.format_exc()

    msg = (
        f"TCP Client Error [{stage}] attempt {attempt}/{retry_counter}\r\n"
        f"Dest=({host}:{port}) Local={local_addr} Sock={sock_info}\r\n"
        f"Reason: {reason}\nHint: {hint}\r\n"
        f"Exception: {type(exc)} errno={getattr(exc,'errno',None)} "
        f"strerror={getattr(exc,'strerror',None)} msg={exc}\r\n"
        f"TCP_INFO: {tcp_info}\r\n"
        f"Command: {command}\r\n"
        # f"Traceback:\n{tb}"
    )
    process_log.add("Error", msg)
    utils_print.output_box(msg)


def _log_deep_timeout(host: str, port: int, sock, command: str,
                      attempt: int, retry_counter: int, start_ts: float, timeout: float) -> None:
    """
    Emit a detailed diagnostic for connect timeouts (final attempt only).
    Includes timing, addresses, socket repr, reason/hint, and TCP_INFO (Linux).
    """
    elapsed = time.time() - start_ts
    sock_info = str(sock) if sock else "Socket not created"
    local_addr = None
    try:
        if sock:
            local_addr = sock.getsockname()
    except OSError:
        pass

    reason, hint = _classify_connect_failure(timed_out=True)
    tcp_info = _get_tcp_info(sock)

    msg = (
        f"TCP Client Error [Timeout] attempt {attempt}/{retry_counter}\r\n"
        f"Dest=({host}:{port}) Local={local_addr} Sock={sock_info}\r\n"
        f"Reason: {reason}\nHint: {hint}\r\n"
        f"Elapsed: {elapsed:.2f}s (limit {timeout}s)\r\n"
        f"TCP_INFO: {tcp_info}\r\n"
        f"Command: {command}"
    )
    process_log.add("Error", msg)
    utils_print.output_box(msg)


def _safe_close(sock) -> None:
    """Close a socket, suppressing errors (prevents FD leaks on failure paths)."""
    try:
        if sock:
            sock.close()
    except OSError:
        pass


def _should_deep(attempt: int, retry_counter: int) -> bool:
    """Return True only on the final attempt to limit deep logs to once per call."""
    return attempt >= max(1, int(retry_counter or 1))


# --------------------------------- Main API ----------------------------------

def socket_open(host: str, port: int, command: str, connect_timeout: float, retry_counter: int):
    """
    Returns a connected blocking socket on success; otherwise logs and returns None.
    - Uses non-blocking connect + select() + SO_ERROR to handle EINPROGRESS correctly.
    - Never raises; concise logs on early retries; deep inspection only on the last attempt.
    - Performs best-effort TCP tuning (NODELAY, KEEPALIVE) on all platforms.
    """
    trace_level = trace_methods.is_traced("tcp out")
    command_str = command.replace("\t", " ")

    # Optional: resolve "self" indirection
    if net_utils.is_use_self(host, port):
        host, port = net_utils.get_self_ip_port()

    # No destination IP
    if host == "0.0.0.0":
        msg = "TCP Client Error: No destination IP for message: " + command_str
        process_log.add("Error", msg)
        utils_print.output_box(msg)
        return None

    attempts = 0
    last_err_msg = None
    total_tries = max(1, int(retry_counter or 1))

    while attempts < total_tries:
        attempts += 1
        sock = None
        start_ts = time.time()

        try:
            # Create socket
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            except Exception:
                etype, e = sys.exc_info()[:2]
                process_log.add_and_print("Error", f"TCP Client: Error socket create: {etype} : {e}")
                return None

            # Basic tuning (non-fatal if any set fails)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                _enable_keepalive(sock)  # portable keepalive
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 20 * params.TCP_BUFFER_SIZE)
            except OSError as e:
                # Tose setsockopt calls are tuning, not required for a connection to work.
                # If any of them fail, the socket can still connect and exchange data using OS defaults—so the code deliberately treats failures as a warning (continue) rather than a hard error (abort).
                pass

            # Non-blocking connect
            sock.setblocking(False)

            # Start connect
            try:
                sock.connect((host, port))
            except BlockingIOError as e:
                # Expected on non-blocking connect if in progress
                if e.errno not in (
                    getattr(errno, "EINPROGRESS", 115),
                    getattr(errno, "EALREADY", 114),
                    getattr(errno, "EWOULDBLOCK", 11),
                    10035,  # Windows WSAEWOULDBLOCK
                ):
                    # Immediate real error
                    last_err_msg = (f"TCP Client Error: #{attempts}/{retry_counter} Failed connection with "
                                    f"{host}:{port} Error: ({type(e)} : [Errno {getattr(e,'errno',None)}] {e}) "
                                    f"message: {command_str}")
                    if _should_deep(attempts, total_tries):
                        _log_deep_error("Immediate connect error", host, port, sock, command_str, e, attempts, retry_counter)
                        _safe_close(sock)
                        return None
                    process_log.add("Error", last_err_msg)
                    _safe_close(sock)
                    time.sleep(6)
                    continue
            except socket.gaierror as e:
                # DNS/resolve failure (rare here because we pass IP, but keep for completeness)
                last_err_msg = (f"TCP Client Error: Name resolution failed for {host}:{port} "
                                f"({type(e)} : [Errno {e.errno}] {e}) - message not delivered: {command_str}")
                if _should_deep(attempts, total_tries):
                    _log_deep_error("DNS resolution", host, port, sock, command_str, e, attempts, retry_counter)
                else:
                    process_log.add_and_print("Error", last_err_msg)
                _safe_close(sock)
                return None
            except Exception as e:
                last_err_msg = (f"TCP Client Error: #{attempts}/{retry_counter} Failed connection with "
                                f"{host}:{port} Error: ({type(e)} : {e}) message: {command_str}")
                if _should_deep(attempts, total_tries):
                    _log_deep_error("Unexpected connect exception", host, port, sock, command_str, e, attempts, retry_counter)
                    _safe_close(sock)
                    return None
                process_log.add("Error", last_err_msg)
                net_utils.test_network_addr(host, port)
                _safe_close(sock)
                time.sleep(6)
                continue

            # Wait for connect completion (writable) within timeout
            writable = select.select([], [sock], [], connect_timeout)[1]
            if not writable:
                last_err_msg = (f"TCP Client Error: Connection with {host}:{port} timed out after "
                                f"{connect_timeout}s - message not delivered: {command_str}")
                if _should_deep(attempts, total_tries):
                    _log_deep_timeout(host, port, sock, command_str, attempts, retry_counter, start_ts, connect_timeout)
                    _safe_close(sock)
                    return None
                process_log.add("Error", last_err_msg)
                _safe_close(sock)
                time.sleep(6)
                continue

            # Check final status via SO_ERROR
            soerr = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if soerr != 0:
                conn_refused = soerr in (getattr(errno, "ECONNREFUSED", 111), 10061)
                err_txt = "Connection Refused" if conn_refused else f"connect failed (SO_ERROR={soerr})"
                last_err_msg = (f"TCP Client Error: {err_txt} : ({host} : {port}) - "
                                f"message not delivered: {command_str}")
                if _should_deep(attempts, total_tries):
                    e = OSError(soerr, os.strerror(soerr) if hasattr(os, "strerror") else "")
                    _log_deep_error("SO_ERROR connect failure", host, port, sock, command_str, e, attempts, retry_counter)
                    _safe_close(sock)
                    return None
                process_log.add("Error", last_err_msg)
                _safe_close(sock)
                time.sleep(6)
                continue

            # Connected → restore blocking mode for send/recv
            sock.setblocking(True)
            sock.settimeout(None)

            # Trace hook
            if trace_level:
                trace_methods.add_details("tcp out",
                    **{"Connect retries": str(attempts - 1), "Socket Connect": str(sock)})

            return sock  # SUCCESS

        except Exception as e:
            # Catch-all safety net
            last_err_msg = (f"TCP Client Error: Unexpected error during connect to {host}:{port} "
                            f"({type(e)} : {e}) - message not delivered: {command_str}")
            if _should_deep(attempts, total_tries):
                _log_deep_error("Generic failure", host, port, sock, command_str, e, attempts, retry_counter)
                _safe_close(sock)
                return None
            process_log.add("Error", last_err_msg)
            _safe_close(sock)
            time.sleep(6)
            continue

    # Final fallback if loop exits without success
    if last_err_msg:
        process_log.add_and_print("Error", last_err_msg)
    else:
        process_log.add_and_print("Error", f"TCP Client Error: Could not connect to {host}:{port} - {command_str}")
    return None

'''
def socket_open(host: str, port: int, command:str, connect_timeout:int, retry_counter:int):

    
    # connect_timeout - the wait time on the soc.connect((host, port) call - it is calculated by the user timeout value
    # retry_counter - the number of times to call soc.connect((host, port) - it is calculated by the user timeout value
    
    ret_val = True
    counter = 0
    trace_level = trace_methods.is_traced("tcp out")

    try:

        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        soc = None
        errno, value = sys.exc_info()[:2]
        err_msg = "TCP Client: Error socket create: {0} : {1}".format(str(errno), str(value))
        process_log.add("Error", err_msg)
        if trace_level:
            details = {"Socket Create": err_msg}
            trace_methods.add_details("tcp out", **details)
        ret_val = False

    else:
        soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)  # setsockopt(level, optname, value)
        soc.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)  # always send the data
        soc.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        soc.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 20 * params.TCP_BUFFER_SIZE)
        # soc.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 10))
        soc.setblocking(True)  # wait for the data to be send (equal to soc.settimeout(None)

        soc.settimeout(connect_timeout)

        if net_utils.is_use_self(host, port):
            # If configuration defined: set self ip = dynamic
            # Change to the IP and Port set to self manage
            host, port = net_utils.get_self_ip_port()

        while ret_val:
            try:
                soc.connect((host, port))
            except:
                counter += 1
                errno, value = sys.exc_info()[:2]
                if retry_counter > 1 and counter == 1 and errno == socket.timeout:
                    continue  # try again once

                command_str = command.replace('\t', ' ')    # remove tabs from error msg

                if host == "0.0.0.0":
                    # This is a reply to a message that did not specified the reply ip and was not available on the socket
                    err_msg = "TCP Client Error: No destination IP for message: " + command_str
                    process_log.add_and_print("Error", err_msg)
                    soc = None
                    ret_val = False
                    break

                connection_refused = value.errno == 10061

                if not connection_refused and counter < retry_counter:
                    # in the case of all threads in the peer node are busy
                    err_msg = "TCP Client Error: #%u/%u Failed connection with %s:%u Error: (%s : %s) message: %s" % (
                    counter, retry_counter, host, port, str(errno), str(value), command_str)
                    process_log.add("Error", err_msg)
                    time.sleep(6)
                    continue

                if connection_refused:
                    err_msg = "TCP Client Error: Connection Refused : (%s : %s) - message not delivered: %s" % (host, port, command_str)
                else:
                    err_msg = "TCP Client Error: Connection with %s:%u failed with error: (%s : %s) - message not delivered: %s" % (host, port, str(errno), str(value), command_str)

                process_log.add("Error", err_msg)
                if soc:
                    socket_info = str(soc)
                else:
                    socket_info = "Socket Info not available"
                err_msg = "TCP Client Error: socket failed to connect: " + socket_info
                process_log.add("Error", err_msg)

                net_utils.test_network_addr(host, port)  # place a message if the ip is in the wrong format
                soc = None
                ret_val = False
            break

    if trace_level:
        details = {
                "Connect retries": str(counter),
                "Socket Connect": str(soc),
                }
        trace_methods.add_details("tcp out", **details)

    if ret_val:
        soc.settimeout(None)        #  If None is given -  the socket is put in blocking mode (for the send).

    # print_soc_process("open", soc)

    return soc

'''
# =======================================================================================================================
# Close socket and place on the free list
#  use shutdown on a socket before you close it. The shutdown is an advisory to the socket at the other end. Depending on the argument you pass it.
# Details - https://docs.python.org/3/howto/sockets.html
# =======================================================================================================================
def socket_close(soc):
    # print_soc_process("close", soc)
    trace_level = trace_methods.is_traced("tcp out")

    try:
        soc.shutdown(socket.SHUT_RDWR)
    except:
        if trace_level:
            errno, value = sys.exc_info()[:2]
            details = {"Socket Shutdown": str(value)}
            trace_methods.add_details("tcp out", **details)
    else:
        if trace_level:
            details = {"Socket Shutdown": "Success"}
            trace_methods.add_details("tcp out", **details)

    try:
        soc.close()
    except:
        if trace_level:
            errno, value = sys.exc_info()[:2]
            details = {"Socket Close": str(value)}
            trace_methods.add_details("tcp out", **details)
    else:
        if trace_level:
            details = {"Socket Close": "Success"}
            trace_methods.add_details("tcp out", **details)

# =======================================================================================================================
# Prepare a message and send data to server
# The info in the message contains 2 parts: Command Part and Data part
# =======================================================================================================================
def message_prep_and_send(err_value, soc, mem_view: memoryview, command: str, auth_data: str, data: str,
                          info_type: int):
    message_header.set_error(mem_view, err_value)  # reset the error code
    message_header.set_info_type(mem_view, info_type)  # the type of info in the block
    message_header.prep_command(mem_view, command)  # add command to the send buffer

    data_encoded = data.encode()
    last_block = False
    block_number = 1
    bytes_transferred = 0

    # if use_authentication:
    if auth_data:
        # send a signed message with the IP, Port and Time that can be authenticated
        if not message_header.set_authentication(mem_view, auth_data):
            process_log.add("Error", "Internal Block Error - no space for authentication")
            return False

    while True:

        offset = message_header.insert_encoded_data(mem_view, data_encoded[bytes_transferred:])

        if not offset:
            last_block = True

        message_header.set_block_number(mem_view, block_number, last_block)

        ret_val = mem_view_send(soc, mem_view)  # Returns False if failed

        if not ret_val or last_block:
            break

        message_header.reset_authentication(mem_view)  # Authentication data is only needed with first message

        # send another block
        block_number += 1
        bytes_transferred += offset

    return ret_val


# =======================================================================================================================
# Send data to server
# =======================================================================================================================
def mem_view_send(soc, mem_view: memoryview):
    # print_soc_process("send", soc)

    trace_network = trace_methods.is_traced("tcp out")

    file_no = soc.fileno()
    if file_no < 0:
        err_msg = "TCP Client failed to send a message, socket not active"
        process_log.add("Error", err_msg)
        utils_print.output(err_msg, True)
        if trace_network:
            params = {
                "Open Socket" : err_msg,
            }
            trace_methods.add_details("tcp out", **params)
            trace_methods.print_details("tcp out", "red")
        return False

    thread_id = utils_threads.get_thread_number()
    message_header.set_send_socket_info(mem_view, 0, None, thread_id)

    ret_val = True
    try:
        soc.settimeout(10)  # 10 --> None
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = "TCP Client failed to send a message, socket error: {0} : {1}".format(str(errno), str(value))
        process_log.add("Error", err_msg)
        utils_print.output(err_msg, True)
        if trace_network:
            params = {"Socket Timeout": err_msg}
            trace_methods.add_details("tcp out", **params)
            trace_methods.print_details("tcp out", "red")

        return False

    if net_utils.get_TCP_debug():
        utils_print.output_box("TCP Client sending data using: %s" % str(soc))

    while 1:
        try:
            soc.sendall(mem_view)
            break
        except:
            errno, value = sys.exc_info()[:2]
            if errno == socket.timeout:
                err_msg = "TCP Client failed to send a message: timed out after 10 seconds"
                process_log.add("Error", err_msg)
                ret_val = False
                break  # replaced from break
            if value.args[0] == net_utils.BROKEN_PIPE:
                err_msg = "TCP Client failed to send a message: BROKEN PIPE"
            else:
                err_msg = "TCP Client failed to send a message: {0} : {1}".format(str(errno), str(value))
            process_log.add("Error", err_msg)
            utils_print.output(err_msg, True)
            ret_val = False
            break

    soc.settimeout(None)

    if trace_network:
        if ret_val:
            params = {"Socket Send": "OK"}
            color = "green"
        else:
            params = {"Socket Send": err_msg}
            color = "red"

        trace_methods.add_details("tcp out", **params)

    return ret_val
# -------------------------------------------------------------
# Print the current process
# -------------------------------------------------------------
def print_soc_process(process, soc):
    name = threading.current_thread().name.ljust(10)[:10]

    text = "\n%s :   %s    :    %s" % (name, process, str(soc))
    utils_print.output(text, False)
