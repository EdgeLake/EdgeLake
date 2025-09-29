"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# PULL Status From Docker

import os
import sys
from socket import gethostname

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_columns as utils_columns

try:
    import docker
    CLIENT = docker.from_env()
except:
    docker_ = False
else:
    docker_ = True

HOSTNAME = os.getenv('HOSTNAME', gethostname())

TITLE_MAP = {
    "UID": "user_id",
    "PID": "process_id",
    "PPID": "parent_process_id",
    "C": "cpu_usage",
    "STIME": "start_time",
    "TTY": "terminal",
    "TIME": "cpu_time",
    "CMD": "command"
}

# ------------------------------------------------------------------------------
# Prepare the windows Event Log Process
# ------------------------------------------------------------------------------
def prep_docker_events(status, prep_params, dispatch_params):
    if not docker_:
        err_msg = "Docker library is not loaded"
        status.add_error(err_msg)
        ret_val = process_status.Failed_to_import_lib
    else:
        err_msg = ""
        ret_val = process_status.SUCCESS
    return [ret_val, err_msg]

# ------------------------------------------------------------------------------
# Get Docker status
# ------------------------------------------------------------------------------
def get_docker_stat(status, dispatch_params):

    containers_info = []

    curent_sec = utils_columns.get_current_time_in_sec()
    timestamp = utils_columns.get_current_utc_time()

    try:

        for container in CLIENT.containers.list():
            stats = container.stats(stream=False)
            top_info = container.top()
            titles = top_info['Titles']
            processes = top_info['Processes']

            created_str = container.attrs['Created']

            created_time_sec = utils_columns.get_time_in_sec(created_str)
            uptime_sec = curent_sec - int(created_time_sec)
            hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(uptime_sec)
            uptime = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

            blkio = get_readable_io(stats.get('blkio_stats', {}).get('io_service_bytes_recursive', []))
            net_stats = stats.get('networks', {})

            for row in processes:
                container_data = {
                    'hostname': HOSTNAME,
                    'container': container.name,
                    'created': created_str,
                    'timestamp': timestamp,
                    'uptime': uptime,
                    'memory_usage_bytes': stats.get('memory_stats', {}).get('usage', 0),
                    'disk_read_bytes': blkio.get('read', 0),
                    'disk_write_bytes': blkio.get('write', 0),
                    'network_rx_packets': sum(net.get('rx_packets', 0) for net in net_stats.values()),
                    'network_tx_packets': sum(net.get('tx_packets', 0) for net in net_stats.values())
                }

                for i, title in enumerate(titles):
                    mapped_key = TITLE_MAP.get(title, None)
                    if mapped_key:
                        container_data[mapped_key] = float(row[i]) if title == 'C' else row[i]

                containers_info.append(container_data)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f"Process 'run scheduled pull' for Docker' failed: {value}"
        status.add_error(err_msg)
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, containers_info]

def get_readable_io(io_stats):
    io_dict = {}
    for entry in io_stats:
        op = entry.get("op")
        value = entry.get("value")
        if op and value is not None:
            io_dict[op.lower()] = value
    return io_dict