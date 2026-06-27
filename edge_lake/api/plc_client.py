"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys
import time
from dataclasses import dataclass
from datetime import datetime

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_json as utils_json
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.generic.utils_print as utils_print

import edge_lake.api.plc_utils as plc_utils
import edge_lake.api.opcua_client as opcua_client
from edge_lake.api import etherip_client
from edge_lake.api import modbus_client  # Modbus TCP

from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.params import get_param


@dataclass
class ModbusPlcStream:
    """Bundled Modbus TCP arguments for process_data (unit, map labels/options, reconnect URL, dynamic mode, client id)."""

    unit: int
    field_labels: dict | None
    map_options: dict | None
    tcp_url: str | None
    dynamic: bool
    client_name: str
    # UNS when namespace = ...: holds SubscriptionInfo (from mqtt_client module, registry-only) + blockchain targets.
    uns_ctx: modbus_client.ModbusUnsStreamContext | None = None


plc_supported_ = ["opcua", "etherip", "modbus"]

# A dictionary on declared clients
clients_info_ = {

}
included_attr_ = ["id","name","source_timestamp","server_timestamp","status_code", "value"]

# -----------------------------------------------------------------------
def is_plc_supported(plc_type):
    global plc_supported_
    return plc_type in plc_supported_


# -----------------------------------------------------------------------
# Close one PLC connector session (OpcUA, EtherIP, or Modbus TCP).
# Used after a streaming read loop in run_plc_client and after one-shot reads in plc_values.
# -----------------------------------------------------------------------
def _disconnect_plc_client(status, plc_type, client, url):
    """
    Disconnect the active client for the given PLC protocol.

    status   — AnyLog process status object (passed through to connector libraries).
    plc_type — "opcua", "etherip", or "modbus" (must match how client was opened).
    client   — handle returned by declare_connection for that protocol.
    url      — endpoint string used to open the session (host:port for Modbus TCP).
    """
    if plc_type == "opcua":
        opcua_client.disconnect_opcua(status=status, connection=client)
    elif plc_type == "etherip":
        etherip_client.disconnect_etherip(status=status, connection=client, url=url)
    elif plc_type == "modbus":
        modbus_client.disconnect_modbus(status=status, client=client, url=url)


# -----------------------------------------------------------------------
# Exit one or more clients
# -----------------------------------------------------------------------
def exit(client_name):

    global clients_info_

    ret_val = process_status.SUCCESS
    if client_name == "all":
        # exit all
        for entry in clients_info_.values():
            entry["status"] = "stop"
    else:
        if client_name in clients_info_:
            clients_info_[client_name]["status"] = "stop"       # The object with the subscription info
        else:
            ret_val = process_status.Failed_opcua_process
    return ret_val

# ----------------------------------------------------------------------
# Get the info on the running clients
# Get plc client
# ----------------------------------------------------------------------
def get_plc_clients(status, io_buff_in, cmd_words, trace):

    global clients_info_

    if not cmd_words[2].startswith("client"):
        return [process_status.ERR_command_struct, None]

    # set reply counters
    output_table = []
    for key, value in  clients_info_.items():
        counter = value["counter"]
        output_table.append((key, value["protocol"], value["status"], value["frequency"], counter))

    title = ["Client Name", "Protocol", "Status", "Frequency", "Reads"]
    output_txt = utils_print.output_nested_lists(output_table, "PLC Clients Status", title, True)

    return [process_status.SUCCESS, output_txt]
# ----------------------------------------------------------------------
# Info returned to the get processes command
# ----------------------------------------------------------------------
def get_status_string(status):
    counts = {"opcua": 0, "modbus": 0, "etherip": 0}
    for entry in clients_info_.values():
        if entry["status"] == "running":
            proto = entry.get("protocol")
            if proto in counts:
                counts[proto] += 1
    parts = []
    if counts["opcua"]:
        noun = "Client" if counts["opcua"] == 1 else "Clients"
        parts.append(f"{counts['opcua']} OPC_UA {noun}")
    if counts["modbus"]:
        noun = "Client" if counts["modbus"] == 1 else "Clients"
        parts.append(f"{counts['modbus']} Modbus {noun}")
    if counts["etherip"]:
        noun = "Client" if counts["etherip"] == 1 else "Clients"
        parts.append(f"{counts['etherip']} EtherNet/IP {noun}")
    info_str = ", ".join(parts)
    return info_str

def is_running():
    clients_count = 0
    for entry in clients_info_.values():
        if entry["status"] == "running":
            clients_count += 1
            break

    return True if clients_count else False
# ------------------------------------------------------------------------------------------------------------------
# OPCUA Client that reads from the OPCUA connector every interval
# run plc client where type = opcua and url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 20
# run plc client where type = opcua and url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 10 and dbms = nov and table = sensor and node = "ns=0;i=2257" and node = "ns=0;i=2258"
# Modbus TCP — example:
# run plc client where type = modbus and hostname = 10.0.0.111 and port = 1502 and device_id = 1 and frequency = 20 and name = modbus_sim and dbms = nov and table = sensor and map = [{"name":"sensor_1","register":0}]
# ------------------------------------------------------------------------------------------------------------------
def _validate_modbus_plc_client(status, conditions, url):
    """
    Modbus TCP only, before any socket: ensure pymodbus is present, command fields are valid,
    build host:port into url, and expand map= into the read list and decode metadata.

    Returns (ret_val, url, id_nodes, modbus_field_labels, modbus_map_options, modbus_unit).
    ret_val is SUCCESS when ok to call declare_connection; otherwise the error code to return from run_plc_client.
    """
    # Ensure pymodbus is present
    ret_lib = modbus_client.test_lib_installed(status)
    if ret_lib:
        return ret_lib, url, None, None, None, None

    # Ensure device_id is specified
    if not modbus_client.modbus_device_id_specified(conditions):
        status.add_error(
            "Run PLC client (modbus): device_id = [Modbus PDU address, typically 0–247] is required"
        )
        return process_status.ERR_command_struct, url, None, None, None, None

    # Ensure node= or nodes= are not used
    if conditions.get("node") or interpreter.get_one_value_or_default(conditions, "nodes", None):
        status.add_error("Run PLC client (modbus): use map= only; do not use node= or nodes=")
        return process_status.ERR_command_struct, url, None, None, None, None

    # Ensure hostname and port are specified
    host_val = interpreter.get_one_value_or_default(conditions, "hostname", None)

    if host_val:
        # Build host:port into url (used for all PLC types
        port_val = int(interpreter.get_one_value_or_default(conditions, "port", 1502))
        url = f"{host_val}:{port_val}"
    else:
        # Ensure hostname and port are specified
        status.add_error("Run PLC client (modbus): specify hostname and port")
        return process_status.ERR_command_struct, url, None, None, None, None

    # Modbus register map from map= (JSON string or parsed value); defines coils/registers to read instead of node=/nodes=.
    map_raw = interpreter.get_one_value_or_default(conditions, "map", None)

    if not map_raw:
        status.add_error("Run PLC client (modbus): map = [JSON array] is required")
        return process_status.ERR_command_struct, url, None, None, None, None

    # Parse map= into Modbus read points (mapped_nodes), column labels, and per-field decode options.
    ret_map, mapped_nodes, modbus_field_labels, modbus_map_options = modbus_client.expand_modbus_register_map(
        status, map_raw
    )

    if ret_map:
        return ret_map, url, None, None, None, None

    # Same name as OPC UA / EtherIP: list of read targets passed to process_data (here, expanded Modbus map entries).
    id_nodes = mapped_nodes

    if not id_nodes:
        status.add_error("Run PLC client (modbus): map produced no register entries")
        return process_status.ERR_command_struct, url, None, None, None, None

    # Modbus PDU unit id from device_id= (required above); passed through to reads.
    modbus_unit = modbus_client.read_modbus_device_id(conditions)

    return process_status.SUCCESS, url, id_nodes, modbus_field_labels, modbus_map_options, modbus_unit


def run_plc_client(dummy: str, conditions: dict):

    global clients_info_

    status = process_status.ProcessStat()
    ret_val = process_status.SUCCESS
    err_msg = None


    prep_dir = get_param("prep_dir")
    watch_dir = get_param("watch_dir")
    err_dir = get_param("err_dir")

    plc_type = conditions["type"]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)
    frequency = interpreter.get_one_value(conditions, "frequency")      # Frequency in MS
    dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
    table_name = interpreter.get_one_value_or_default(conditions, "table", None)
    id_nodes = conditions.get("node", None)
    if not id_nodes:
        nodes_list = interpreter.get_one_value_or_default(conditions, "nodes", None)
        if nodes_list:
            # A list specified in the command line  (Option B)
            id_nodes = utils_json.str_to_json(nodes_list)

    topic_name = interpreter.get_one_value_or_default(conditions, "topic", None)
    policy_id = interpreter.get_one_value_or_default(conditions, "policy", None)
    plc_dynamic = interpreter.get_one_value_or_default(conditions, "dynamic", False)

    modbus_field_labels = None
    modbus_map_options = None
    modbus_unit = None

    # Validate client configuration
    if plc_type == "modbus":
        ret_mbus, url, id_nodes, modbus_field_labels, modbus_map_options, modbus_unit = _validate_modbus_plc_client(
            status, conditions, url
        )
        if ret_mbus != process_status.SUCCESS:
            return ret_mbus
    elif plc_type in ("opcua", "etherip"):
        if url is None or not str(url).strip():
            status.add_error(
                "Run PLC client: url = [connection string] is required when type = opcua or etherip"
            )
            return process_status.ERR_command_struct

    # info for this client
    info_dict = {
        "protocol" : plc_type,
        "status": "running",
        "frequency": frequency,
        "counter" : 0,
    }


    client_name = conditions["name"][0]

    if not client_name in clients_info_ or clients_info_[client_name]["status"] == "terminated":
        clients_info_[client_name] = info_dict
        if topic_name:
            # Flag this is placed on the local broker
            ret_val, user_id = mqtt_client.register(status, {"broker": ["local"]})
        else:
            user_id = None
    else:
        status.add_error(f"Multiple PLC Clients with client name: {client_name}")
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        # Establish connection to the OPC-UA server
        if plc_type == "opcua":
            client = opcua_client.declare_connection(status=status, url=url, user=user, password=password)
        elif plc_type == "etherip":
            client = etherip_client.declare_connection(status, url, user, password)
        elif plc_type == "modbus":
            client = modbus_client.declare_connection(status, url, user, password, client_name=client_name)

        if not client:
            info_dict["status"] = "Error"
            status.add_error(f"Failed to connect ({plc_type}) using: {url}")
            if plc_type == "modbus":
                return process_status.Failed_PLC_CONNECT
            return process_status.Failed_OPC_CONNECT

        ret_val = plc_utils.set_tag_info(status)  # Copy tag metadata
        if ret_val:
            info_dict["status"] = "Error"
            if plc_type == "modbus":
                # Close and return.
                _disconnect_plc_client(status, plc_type, client, url)
            return ret_val

        modbus_uns_ctx = None
        if plc_type == "modbus" and plc_dynamic:
            ret_uns, modbus_uns_ctx = modbus_client.modbus_plc_resolve_dynamic_uns_context(
                status,
                plc_type,
                dbms_name,
                plc_dynamic,
                (interpreter.get_one_value_or_default(conditions, "namespace", None) or "").strip(),
                conditions,
            )
            if ret_uns:
                info_dict["status"] = "Error"
                _disconnect_plc_client(status, plc_type, client, url)
                return ret_uns

        # Read according to the frequency
        while True:
            start_time = time.time()

            bump_reads = True
            if id_nodes:
                # Read PLC values for this connector (plc_type) and publish or buffer via process_data.
                modbus_ctx = None
                if plc_type == "modbus":
                    modbus_ctx = ModbusPlcStream(
                        unit=modbus_unit,
                        field_labels=modbus_field_labels,
                        map_options=modbus_map_options,
                        tcp_url=url,
                        dynamic=plc_dynamic,
                        client_name=client_name,
                        uns_ctx=modbus_uns_ctx,
                    )
                ret_val = process_data(status, plc_type, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name, modbus=modbus_ctx)
                # modbus returns PLC_modbus_empty_poll when no points were read successfully, due to connection errors or modbus data errors.
                if ret_val and ret_val != process_status.PLC_modbus_empty_poll:
                    status.add_error(
                        f"{plc_type} client '{client_name}' terminated due to error: {process_status.get_status_text(ret_val)}"
                    )
                    break
                # if ret_val is SUCCESS, then we can bump the reads counter
                bump_reads = ret_val == process_status.SUCCESS

            if info_dict["status"] == "stop":
                break

            if bump_reads:
                info_dict["counter"] += 1

            diff_time =  time.time() - start_time
            if diff_time < frequency:
                time.sleep(frequency - diff_time)

        # Close the PLC session (OPC UA, EtherNet/IP, or Modbus) after the read loop ends.
        _disconnect_plc_client(status, plc_type, client, url)

    if topic_name:
        # if local broker is used
        mqtt_client.end_subscription(user_id, True)

    utils_print.output("PLC client process terminated: %s" % process_status.get_status_text(ret_val), True)
    if err_msg:
        utils_print.output_box(err_msg)

    info_dict["status"] = "terminated"

# ------------------------------------------------------------------------------------------------------------------
# Read data and send to broker ot buffers
# ------------------------------------------------------------------------------------------------------------------
def process_data(status, plc_type, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name, modbus: ModbusPlcStream | None = None):
    # --- Modbus ingest (other plc_type paths ignore modbus / modbus_dynamic_* ) ---
    # dynamic = false (default): command has table = ... → single_table True. The read loop fills one json_row
    # with one column per map "name" (plus timestamp/duration). Then add_data streams that wide row to table.
    # If every point failed, json_row has no data columns → has_payload is false → PLC_modbus_empty_poll (poll
    # again; run_plc_client does not bump Reads).
    #
    # dynamic = true: omit table = → single_table False. Each map point is streamed separately via
    # modbus_dynamic_publish_row to {client_name}_{map name}, or via UNS when namespace = ... is set.
    # modbus_dynamic_had_row tracks any success.
    # If the loop had no error but nothing was streamed → PLC_modbus_empty_poll (same polling semantics).

    _plc_label = {"opcua": "OPC UA", "etherip": "EtherNet/IP", "modbus": "Modbus TCP"}.get(plc_type, plc_type)

    if plc_type == "modbus" and modbus is None:
        status.add_error(f"{_plc_label}: missing Modbus stream context")
        return process_status.ERR_process_failure

    plc_dynamic = modbus.dynamic if modbus else False
    modbus_unit = modbus.unit if modbus else None
    modbus_field_labels = modbus.field_labels if modbus else None
    modbus_map_options = modbus.map_options if modbus else None
    modbus_tcp_url = modbus.tcp_url if modbus else None
    plc_client_name = modbus.client_name if modbus else None
    modbus_uns_ctx = modbus.uns_ctx if modbus else None

    def _entry_ref(entry):
        """Label for error messages: OpcUA/EtherIP rows use tuple index 1; Modbus rows use .tag."""
        if isinstance(entry, (list, tuple)) and len(entry) > 1:
            return entry[1]
        return getattr(entry, "tag", repr(entry))

    def _entry_val_ref(entry):
        """Value snippet for error messages: tuple index 5 for legacy rows; Modbus uses .value."""
        if isinstance(entry, (list, tuple)) and len(entry) > 5:
            return entry[5]
        return getattr(entry, "value", "?")

    if table_name:
        # One destination table (Modbus default with dynamic = false uses this path).
        single_table = True
        json_row = {
            "timestamp" : None,             # First timestamp
            "duration" : 0,
        }
    else:
        # No command table: Modbus + dynamic = true uses per-map tables; opcua/etherip use tag policies (blockchain).
        single_table = False
        json_row = {}

    if plc_type == "modbus" and plc_dynamic and single_table:
        status.add_error(
            f"{_plc_label}: dynamic = true cannot be combined with table = ... (omit table =)"
        )
        return process_status.ERR_process_failure

    ret_val = process_status.SUCCESS

    if plc_type == "opcua":
        multiple_values = opcua_client.get_multiple_opcua_values(status, client, id_nodes, ["all"], "collection", False)
        get_plc_tag_info = opcua_client.get_plc_tag_info      # A method returning attr name, attr value, timestamp
    elif plc_type == "etherip":
        ret_val, multiple_values = etherip_client.read_data(status, client, id_nodes)
        if ret_val:
            return ret_val
        if not isinstance(multiple_values,list):
            multiple_values = [multiple_values]             # A single value is not returned in a list
        get_plc_tag_info = etherip_client.get_plc_tag_info  # A method returning attr name, attr value, timestamp
    elif plc_type == "modbus":
        ret_val, multiple_values = modbus_client.read_data(
            status,
            client,
            id_nodes,
            modbus_client.ModbusReadContext(
                unit=modbus_unit,
                map_options=modbus_map_options,
                field_labels=modbus_field_labels,
                tcp_url=modbus_tcp_url,
            ),
        )
        if ret_val:
            return ret_val
        get_plc_tag_info = modbus_client.get_modbus_tag_info
    else:
        multiple_values = None
        get_plc_tag_info = None

    if multiple_values:
        # Shared path: opcua / etherip via get_plc_tag_info; modbus uses it too unless map= supplied column labels.
        timestamp_first = None
        timestamp_last = None
        modbus_dynamic_had_row = False
        for entry in multiple_values:
            if plc_type == "modbus" and getattr(entry, "error", None):
                # This tag read failed; omit that column from this row (do not abort the whole row).
                continue
            # Gets the info from each connector
            if plc_type == "modbus" and modbus_field_labels:
                tag_key, timestamp, attr_name, attr_val = modbus_client.get_modbus_tag_info(
                    entry, modbus_field_labels.get(entry.tag)
                )
            else:
                tag_key, timestamp, attr_name, attr_val = get_plc_tag_info(entry)

            if not timestamp:
                status.add_error(f"{_plc_label}: failed to retrieve timestamp for entry '{_entry_ref(entry)}'")
                ret_val = process_status.ERR_process_failure
                break

            if not timestamp_first:
                timestamp_first = timestamp        # take source_timestamp if available or server timestamp (second option)
                timestamp_last = timestamp_first
            else:
                if timestamp < timestamp_first:
                    timestamp_first = timestamp
                elif timestamp > timestamp_last:
                    timestamp_last = timestamp
            if not attr_name:
                status.add_error(f"{_plc_label}: failed to process entry '{_entry_ref(entry)}' with value {_entry_val_ref(entry)}")
                ret_val = process_status.ERR_process_failure
                break

            if single_table:
                # A single table to cover the tag data
                if plc_type == "modbus" and attr_val is None:
                    # Do not insert null placeholders for failed Modbus points.
                    continue
                json_row[attr_name] = attr_val
            else:
                # Modbus dynamic: one streamed row per map point (``name =`` + map ``name``, or UNS if ``namespace`` is set).
                if plc_type == "modbus" and plc_dynamic:
                    if attr_val is None:
                        continue
                    dyn_ret, dyn_ins = modbus_client.modbus_dynamic_publish_row(status, _plc_label, dbms_name, plc_client_name, modbus_field_labels, entry, timestamp_first, attr_val, prep_dir, watch_dir, err_dir, modbus_uns_ctx)
                    if dyn_ret:
                        ret_val = dyn_ret
                        break
                    if dyn_ins:
                        modbus_dynamic_had_row = True
                    continue

                if not tag_key in plc_utils.tag_info_:
                    err_msg = f"{_plc_label}: no tag policy matches key {tag_key}"
                    status.add_error(err_msg)
                    utils_print.output_box(err_msg)
                    ret_val = process_status.ERR_process_failure
                    break
                else:
                    policy_inner = plc_utils.tag_info_[tag_key]
                    if dbms_name:
                        # User provided dbms name
                        target_dbms = dbms_name
                    elif "dbms" in policy_inner:
                        # Tag policy provided dbms name
                        target_dbms = policy_inner["dbms"]
                    else:
                        policy_id = policy_inner["id"] if "id" in policy_inner else "(Policy is missing an ID)"
                        status.add_error(f"{_plc_label}: dbms name not specified in 'tag' policy {policy_id}")
                        ret_val = process_status.ERR_process_failure
                        break
                    if "table" in policy_inner:
                        # Tag policy provided dbms name
                        target_table = policy_inner["table"]
                    else:
                        policy_id = policy_inner["id"] if "id" in policy_inner else "(Policy is missing an ID)"
                        status.add_error(f"{_plc_label}: table name not specified in 'tag' policy {policy_id}")
                        ret_val = process_status.ERR_process_failure
                        break

                    # different dbms and table per inset
                    if timestamp_first:
                        # Add timestamp on the row - representing the first timestamp and the duration
                        try:
                            if isinstance(timestamp_first, str):
                                json_row["timestamp"] = timestamp_first
                            else:
                                # ISO-8601 UTC (OpcUA-style); other connectors may also supply datetime here
                                json_row["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        except:
                            pass
                    if not "timestamp" in json_row:
                        err_msg = f"{_plc_label}: failed to set a timestamp attribute using: '{timestamp_first}'"
                        status.add_error(err_msg)
                        utils_print.output_box(err_msg)
                        break
                    json_row["value"] = attr_val
                    json_msg = utils_json.to_string(json_row)
                    if not json_msg:
                        status.add_error(f"{_plc_label}: failed to process JSON row: '{str(json_row)}'")
                        ret_val = process_status.ERR_process_failure
                        break
                    ret_val, hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, target_dbms,
                                                   target_table, '0', '0', "json", json_msg)
                    if ret_val:
                        break

        # Modbus empty poll (keep polling, do not count Reads):
        # - dynamic = false + table = ... → handled below in single_table via has_payload (wide row had no columns).
        # - dynamic = true (per-map tables) → here: loop had no error but modbus_dynamic_publish_row never ran successfully.
        if plc_type == "modbus" and plc_dynamic and not ret_val and not modbus_dynamic_had_row:
            return process_status.PLC_modbus_empty_poll

        if single_table:
            # Wide row path: finish timestamp/duration, then Modbus checks has_payload before add_data.
            if timestamp_first:
                # Add timestamp on the row - representing the first timestamp and the duration
                try:
                    json_row["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    json_row["duration"] = int((timestamp_last - timestamp_first).total_seconds() * 1000)
                except:
                    pass

            if not ret_val:
                if plc_type == "modbus":
                    has_payload = any(key not in ("timestamp", "duration") for key in json_row.keys())
                    if not has_payload:
                        # No successful Modbus points in this cycle (keep polling; run_plc_client does not count Reads).
                        return process_status.PLC_modbus_empty_poll
                json_msg = utils_json.to_string(json_row)
                if not json_msg:
                    status.add_error(f"{_plc_label}: failed to process JSON for streaming row")
                    ret_val = process_status.ERR_process_failure
                else:
                    if topic_name:
                        ret_val = mqtt_client.process_message(topic_name, user_id, json_msg)  # Transfer data to MQTT Client
                    else:
                        ret_val, hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, dbms_name, table_name, '0', '0', "json", json_msg)
    else:
        status.add_error(f"{_plc_label}: no values returned from connector read cycle")
        ret_val = process_status.Failed_PLC_INFO

    return ret_val


# ---------------------------------------------------------------------------------------
# Extract opcua nodes
#
# Command:
#  OPTION A: get opcua values where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and node = "ns=0;i=2257" and node = "ns=0;i=2258"
#  OPTION B: get opcua values where url=opc.tcp://10.0.0.78:4840/freeopcua/server/andoutput=cmdandnode=ns=2;i=1 and list = ["ns=2;i=1", "ns=2;i=2"]

# get plc values where type = etherip and url = 127.0.0.1 and nodes = ["CombinedChlorinatorAI.PV", "STRUCT.Status"]
# ---------------------------------------------------------------------------------------
def plc_values(status, io_buff_in, cmd_words, trace):

    keywords = {"url":                      ("str",     False,  False,  True),      # OPCUA / EtherIP URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                "include":                  ("str",     False,  False, False),      # Additional attributes: name, SourceTimestamp, ServerTimestamp, StatusCode
                "node":                     ("str",     False,  True,  False),      # One or more namespace + id: "ns=2;i=1002"
                "nodes":                    ("str",     False,  True,  True),       # A list of nodes: nodes = ["ns=2;i=1", "ns=2;i=2"]
                "method":                   ("str",     False,  False,  True),      # 2 options: "collection" - 1) a single call to all points 2) "single" - a call for each node
                "failures":                 ("bool",    False,  False, True),       # Default is false. If set to True and executed with method = individual, only errors are returned
                "type":                     ("str",     False,  False, True),       # PLC Type: opcua or etherip
                # --- Modbus TCP (extra keys; use with type = modbus or get modbus values) ---
                "hostname":                 ("str",     False,  False,  True),
                "port":                     ("int",     False,  False,  True),
                "device_id":                ("int",     False,  False,  True),
                "map":                      ("str",     False,  False,  True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    # Modbus TCP — "get modbus values" alias: inject type before the rest of validation
    if len(cmd_words) >= 3 and str(cmd_words[1]).lower() == "modbus" and str(cmd_words[2]).lower() == "values" and "type" not in conditions:
        conditions["type"] = "modbus"

    _t = interpreter.get_one_value(conditions, "type")
    plc_type = _t if _t is not None else "opcua"
    conditions["type"] = plc_type
    
    if not is_plc_supported(plc_type):
        status.add_error(f"Get PLC Values cmd: Wrong connector 'type': {plc_type}")
        return [process_status.ERR_command_struct, None]

    read_method = interpreter.get_one_value_or_default(conditions, "method", "collection")

    if plc_type != "modbus":
        if not counter:
            status.add_error("Missing specific nodes or a list of nodes")
            return [process_status.ERR_process_failure, None]
        if read_method != "collection" and read_method != "individual":
            status.add_error("Get OPCUA values command method must be 'collection' or 'individual'")
            return [process_status.ERR_command_struct, None]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)

    # Modbus TCP — build pymodbus host:port; opcua/etherip keep url=
    if plc_type == "modbus":
        host_val = interpreter.get_one_value_or_default(conditions, "hostname", None)
        if host_val:
            port_val = int(interpreter.get_one_value_or_default(conditions, "port", 1502))
            url = f"{host_val}:{port_val}"
        else:
            status.add_error("Get PLC values (modbus): specify hostname and port")
            return [process_status.ERR_command_struct, None]
        if not modbus_client.modbus_device_id_specified(conditions):
            status.add_error(
                "Get PLC values (modbus): device_id = [Modbus PDU address, typically 0–247] is required"
            )
            return [process_status.ERR_command_struct, None]
    elif not url:
        status.add_error("Missing url in get plc values command")
        return [process_status.ERR_command_struct, None]

    attr_included = conditions.get("include", None)
    if attr_included:
        if attr_included[0] == "all":
            title = included_attr_
        else:
            for entry in attr_included:
                if entry not in included_attr_:
                    status.add_error(f"OPCUA: Wrong 'include' attribute in get opcua values command: '{entry}'")
                    return [process_status.ERR_process_failure, None]
            title = attr_included
            if not "value" in attr_included:
                title += ["value"]      # Always include value
    else:
        title = ["value"]

    # If set to True and executed with method = individual, only errors are returned
    failures = interpreter.get_one_value_or_default(conditions, "failures", False)

    # Establish connection to the OPC-UA server
    if plc_type == "opcua":
        ret_val = opcua_client.test_lib_installed(status)
        if ret_val:
            return [ret_val, None]
        client = opcua_client.declare_connection(status=status, url=url, user=user, password=password)
    elif plc_type == "etherip":
        ret_val = etherip_client.test_lib_installed(status)
        if ret_val:
            return [ret_val, None]
        client = etherip_client.declare_connection(status, url, user, password)
    elif plc_type == "modbus":
        ret_val = modbus_client.test_lib_installed(status)
        if ret_val:
            return [ret_val, None]
        client = modbus_client.declare_connection(status, url, user, password)

    if not client:
        if plc_type == "modbus":
            return [process_status.Failed_PLC_CONNECT, None]
        return [process_status.Failed_OPC_CONNECT, None]

    modbus_field_labels = None
    modbus_map_options = None
    nodes_list = interpreter.get_one_value_or_default(conditions, "nodes", None)
    id_nodes = None

    if plc_type == "modbus":
        # Modbus TCP — expand map = into read targets and decode metadata (get plc values: map only).
        map_raw = interpreter.get_one_value_or_default(conditions, "map", None)
        if map_raw:
            ret_map, mapped_nodes, modbus_field_labels, modbus_map_options = modbus_client.expand_modbus_register_map(
                status, map_raw
            )
            if ret_map:
                # Close and return.
                _disconnect_plc_client(status, plc_type, client, url)
                return [ret_map, None]
            id_nodes = mapped_nodes
        if not id_nodes:
            status.add_error("Get PLC values (modbus): map = [JSON array] is required")
            # Close and return.
            _disconnect_plc_client(status, plc_type, client, url)
            return [process_status.ERR_process_failure, None]
    elif nodes_list:
        # A list specified in the command line  (Option B)
        id_nodes = utils_json.str_to_json(nodes_list)
    else:
        # User is not using a list (Option A)
        id_nodes = conditions["node"]

    ret_val = process_status.SUCCESS
    multiple_values = None
    if plc_type == "opcua":
        multiple_values = opcua_client.get_multiple_opcua_values(status, client, id_nodes, attr_included, read_method, failures)
    elif plc_type == "etherip":
        ret_val, multiple_values = etherip_client.read_data(status, client, id_nodes)
        if ret_val:
            return [ret_val, None]
        if not isinstance(multiple_values, list):
            multiple_values = [multiple_values]  # A single value is not returned in a list
    elif plc_type == "modbus":
        # Modbus TCP
        modbus_unit = modbus_client.read_modbus_device_id(conditions)
        ret_val, multiple_values = modbus_client.read_data(
            status,
            client,
            id_nodes,
            modbus_client.ModbusReadContext(
                unit=modbus_unit,
                map_options=modbus_map_options,
                field_labels=modbus_field_labels,
                tcp_url=url,
            ),
        )
        if ret_val:
            # Close and return.
            _disconnect_plc_client(status, plc_type, client, url)
            return [ret_val, None]

    if not multiple_values:
        ret_val = process_status.Failed_opcua_process
        output_txt = None
    else:
        output_txt = None
        if plc_type == "opcua":
            output_txt = utils_print.output_nested_lists(multiple_values, "OPCUA Nodes values", title, True)
        if plc_type == "etherip":
            output_table = etherip_client.set_tags_output_table(None, id_nodes, True, multiple_values)
            etherip_title = ["Index", "Name", "Key", "Type", "Value", "Error"]
            output_txt = utils_print.output_nested_lists(output_table, "Ethernet/IP Nodes values", etherip_title, True)
        if plc_type == "modbus":
            # Modbus TCP — tabular output
            rows = []
            for entry in multiple_values:
                lbl = modbus_field_labels.get(entry.tag) if modbus_field_labels else None
                _tk, _ts, col, val = modbus_client.get_modbus_tag_info(entry, lbl)
                rows.append([col, entry.tag, val, getattr(entry, "error", None)])
            output_txt = utils_print.output_nested_lists(rows, "Modbus TCP values", ["Column", "Tag", "Value", "Error"], True)

    # Tear down the connector session (opcua, etherip, or modbus) before returning command output.
    _disconnect_plc_client(status, plc_type, client, url)

    return [ret_val, output_txt]
