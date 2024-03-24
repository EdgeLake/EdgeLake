"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.dbms.db_info as db_info
import edge_lake.blockchain.metadata as metadata
import edge_lake.generic.process_status as process_status
import edge_lake.tcpip.message_header as message_header
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_io as utils_io

safe_tsd_ids_ = {}       # Keeps the safe ID (validated_id) from every node

peer_consumers_ = {}     # The IP and Ports of the peers that are sending  get_recent_tsd_info messages

'''
Overview: 
    1) Consumer requests (from each cluster ember) the list of files updated in the tsd table
    2) recent_tsd_info() - receives a list of files updated on peer nodes and updates the metadata in  metadata.update_member_status()
    3) Consumer requests the missing files in alconsumer.get_missing_files
'''
# ----------------------------------------------------------
# This call is Triggered by ALCONSUMER.get_recent_tsd_info on the peer node.
# The peer is sending an event called: event_request_tsd_update
#
# Query TSD info for the most updates row id and time ==>
# return a message (event) with the info
# This process is being triggered by a source machine at alconsumer.event_request_tsd_update
# ----------------------------------------------------------
def get_recent_tsd_info(status, io_buff_in, data, trace):


    global peer_consumers_ # The IP and Ports of the peers that are sending  get_recent_tsd_info messages

    table_name, start_date, end_date = data

    member_id = metadata.get_node_member_id()
    if not member_id:
        member_cmd.blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)
        member_id = metadata.get_node_member_id()  # Get ID of current node after the load

    if not member_id or str(member_id) == table_name[4:]:
        tsd_name = "tsd_info"  # Query the data managed by this node
    else:
        tsd_name = table_name  # Query the data managed by a different node

    ret_val = process_status.SUCCESS
    first_id = 0
    first_date = '0'
    # Get start date info
    if start_date != '0':
        # find before or equal to the start date
        sql_stmt = "<01>select file_id, <TIMESTAMP_START>file_time<TIMESTAMP_END> from %s where file_time <= '%s' order by file_id desc limit 1" % (
        tsd_name, start_date)
        ret_val, data = db_info.tsd_info_select(status, tsd_name, sql_stmt)
        if not ret_val:
            if len(data):
                # TSD table has data earlier than start_date
                first_id = data[0][0]
                first_date = data[0][1]
            else:
                # TSD table without data earlier than start_date
                # find the first file id
                sql_stmt = "<01>select file_id, <TIMESTAMP_START>file_time<TIMESTAMP_END> from %s order by file_id limit 1" % tsd_name
                ret_val, data = db_info.tsd_info_select(status, tsd_name, sql_stmt)
                if not ret_val:
                    if len(data):
                        first_id = data[0][0]
                        first_date = data[0][1]
                        if first_date > start_date:
                            first_id -= 1  # This row is going to be the validated row on the target machine
        elif ret_val == process_status.Empty_result_set:
            # No data in TSD table
            ret_val = process_status.SUCCESS  # Not a real error
        else:
            # Error in query
            status.add_error(
                "Event error: Query of table '%s' from a local database returned error #%u" % (table_name, ret_val))

    if first_date != '0':  # Otherwise - there is no data - no need to send message
        last_id = 0
        if end_date != '0':
            sql_stmt = "<01>select file_id, <TIMESTAMP_START>file_time<TIMESTAMP_END> from %s where file_time >= '%s' order by file_id desc limit 1" % (
            tsd_name, end_date)
            ret_val, data = db_info.tsd_info_select(status, tsd_name, sql_stmt)

            if not ret_val and len(data):
                last_id = data[0][0]
                last_date = data[0][1]

        if not last_id and (not ret_val or ret_val == process_status.Empty_result_set):

            # No end date or nothing before the end date
            # find the last file id
            sql_stmt = "<01>select file_id, <TIMESTAMP_START>file_time<TIMESTAMP_END> from %s order by file_id desc limit 1" % tsd_name
            ret_val, data = db_info.tsd_info_select(status, tsd_name, sql_stmt)

            if not ret_val:
                last_id = data[0][0]
                last_date = data[0][1]

        if not ret_val:

            tsd_net_msg = metadata.get_safe_tsd_ids()   # Get the safe ID for each TSD table

            ip_port = message_header.get_source_ip_port_string(io_buff_in)

            reply_message = ["run", "client", ip_port, "event", "recent_tsd_info", table_name, first_date,
                             str(first_id), last_date, str(last_id), tsd_net_msg]
            ret_val = member_cmd.run_client(status, io_buff_in, reply_message, trace)

            if not ip_port in peer_consumers_:
                # The first message received from the peer node -
                # Wake the consumer to request a sync (otherwise the consumer will wait for a minute)
                process_status.set_signal("consumer")
                peer_consumers_[ip_port] = True


    return ret_val
# ----------------------------------------------------------
# A reply to get_recent_tsd_info ==>
# update the local metadata with the info
# ----------------------------------------------------------
def recent_tsd_info(status, io_buff_in, data, trace):

    global safe_tsd_ids_    # Keeps the safe ID (validated_id) from every node

    table_name, first_date, first_id, last_date, last_id, tsd_net_msg = data

    first_date = first_date.replace('\t', ' ')
    last_date = last_date.replace('\t', ' ')

    metadata.update_member_status(table_name, first_date, first_id, last_date, last_id)

    # Keep the SAFE ID (validated_id) of every node

    if len(tsd_net_msg) > 1:
        # Is with info on TSD tables
        tsd_list = tsd_net_msg[1:-1].split('*')   # Every entry is TSD table + Safe ID (validated_id), i.e.: *tsd_13~6*
        for entry in tsd_list:
            index =  entry.find('~')       # Split between tsd name and the validated_id
            if index > 0 and index < (len(entry) - 1):
                # TSD_NAME ~ validated_id
                tsd_name = entry[:index]
                validated_id = int(entry[index + 1:])

                if not tsd_name in safe_tsd_ids_:
                    safe_tsd_ids_[tsd_name] = {}        # for every TSD table - show the safe ID on each node

                # For each tsd_table - keep the Safe ID on Every node of the cluster
                safe_tsd_ids_[tsd_name][table_name] = validated_id  # The table_name represents the node delivering the message

# ----------------------------------------------------------
# This node requested an archive file from a source node.
# If the source node does not have the file, it replies with
# missing_archived_file message.
# --> TSD table is updated with file not available message
# set Status 1 as - "01" -  "Missing archived data with member X"
# ----------------------------------------------------------
def process_missing_archived_file(status, io_buff_in, data, trace):

    tsd_name, file_missing = data
    file_info = file_missing.split('.')

    if len(file_info) >= 9:

        trace_level = member_cmd.commands["run data consumer"]['trace']         # This message is a reply to a request of the consumer
        if trace_level:
            utils_print.output("\r\n[Message: missing_archived_file] [TSD: %s] [ID: %s] [DBMS: %s] [Table: %s]" % (tsd_name, file_info[6], file_info[0], file_info[1]), True)

        utc_date_time = utils_io.key_to_utc_timestamp(file_info[7])
        sql_insert = "insert into %s (file_id, dbms_name, table_name, source, file_hash, instructions, file_time, rows, status1, status2) "\
                " values ('%s','%s', '%s', '%s','%s', '%s', '%s', %s, '%s', '%s');" % (tsd_name,\
                    file_info[6], file_info[0], file_info[1], file_info[2], file_info[3], file_info[4], utc_date_time, 0, "01", "Missing archived data with member %s" % file_info[5])
        ret_val = db_info.process_contained_sql_stmt(status, "almgm", sql_insert)

    return process_status.SUCCESS

# ----------------------------------------------------------
# Send a message with a list of files that were not copied in the
# process member_cmd.deliver_files.
# ----------------------------------------------------------
def send_missing_arcived_file(status, io_buff_in, ip, port, tsd_table, file_missing, trace):

    event_missing_file = ["run", "client", "(destination)", "event", "missing_archived_file", tsd_table, file_missing]

    event_missing_file[2] = "(%s:%u)" % (ip, port)
    ret_val = member_cmd.run_client(status, io_buff_in, event_missing_file, trace)
    return ret_val
# ----------------------------------------------------------
# A metadata ping only returns metadata_pong to show server is running
# ----------------------------------------------------------
def metadata_ping(status, io_buff_in, data, trace):
    # reply with "pong message"
    event_pong = ["run", "client", "(destination)", "event", "metadata_pong"]

    mem_view = memoryview(io_buff_in)
    ip, port = message_header.get_source_ip_port(mem_view)

    event_pong[2] = "(%s:%s)" % (ip, str(port))
    member_cmd.run_client(status, io_buff_in, event_pong, trace)

    return process_status.SUCCESS

# ----------------------------------------------------------
# A reply for ping - update that the replying server is running
# ----------------------------------------------------------
def metadata_pong(status, io_buff_in, data, trace):

    mem_view = memoryview(io_buff_in)
    ip, port = message_header.get_source_ip_port(mem_view)

    metadata.set_operator_status(status, ip, port, "active", None)

    return process_status.SUCCESS

# ----------------------------------------------------------
# Add the validated ID on each peer node in the cluster
# safe_ids is the dictionary with the Validated IDs on this node
# ----------------------------------------------------------
def add_peers_safe_ids(safe_ids, tsd_table):

    global safe_tsd_ids_

    for tsd_name, values in safe_tsd_ids_.items():

        if tsd_table != 'all' and tsd_name != tsd_table:
            continue        # Only one table

        if not tsd_name in safe_ids:
            # Other nodes reported on the TSD table, but it is not available on this node
            member_id = metadata.get_node_member_id()
            safe_ids[tsd_name] = {
                str(member_id) : 0,
                "consensus": 0  # This value is the common (lowest) Safe ID among all nodes of the cluster
            }

        tsd_safe_ids = safe_ids[tsd_name]       # The safe IDs for one TSD Table
        for peer_id, safe_id in values.items():
            # Values has an entry for each peer node in the cluster, and the validated ID on the peer node
            tsd_safe_ids[peer_id[4:]] = safe_id     # Place the value of the peer in the dictionary
            if safe_id < tsd_safe_ids["consensus"] or not tsd_safe_ids["consensus"]:
                # if tsd_safe_ids["consensus"] == 0 --> this is a TSD of this node which has all the updates
                tsd_safe_ids["consensus"] = safe_id # Place the Safe ID that all cluster members need to use with the tsd_name


