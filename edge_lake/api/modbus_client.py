"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/

Modbus TCP client: connect by hostname:port (commands use hostname= and port=), read tags on a schedule (pymodbus).

- Connect / read failures are reported with status.add_error → AnyLog Error log.
- Client: ModbusTcpClient(host=, port=), connect(), read_* , close().
- Modbus PDU address: set ``device_id`` on commands (default 0 on TCP for many single-device servers).
  map accepts a JSON array of item objects.
  Example:
  map = [
    {"name":"coil_0","coil":0},
    {"name":"coil_1","coil":1},
    {"name":"contact_1","input":0},
    {"name":"data_1","inputRegister":0},
    {"name":"sensor_1","register":0}
  ]
  inputRegister block examples:
    {"name":"raw_data_in","inputRegister":[0,1,2,3,4,5,6,7],"type":"byte"}
    {"name":"voltage_1","inputRegister":[0,1],"type":"long"}
  register block examples:
    {"name":"raw_status","register":[0,1,2,3,4,5,6,7],"type":"byte"}
    {"name":"uptime,"register":[0,1],"type":"long"}
    {"name":"temperature","register":[2,3],"type":"float"}
  Object type to map property:
  - Coil            | 1 bit   | 00001 - 09999 | coil
  - Discrete Input  | 1 bit   | 10001 - 19999 | input
  - Input Register  | 16 bits | 30001 - 39999 | inputRegister
  - Holding Register| 16 bits | 40001 - 49999 | register
  register | inputRegister | coil | input.
  Modifiers apply only to register/inputRegister items:
  - type: long | float | byte
    - long requires 1..4 consecutive registers
    - byte requires more than 4 consecutive registers
  - swap: bytes | words | both | none
  - scale / offset: numeric (forces float output/type)
  coil/input do not support modifiers and are emitted as numeric 0/1.

"""

import logging
import re
import struct
import sys
import threading
from dataclasses import dataclass
from types import SimpleNamespace

try:
    from pymodbus.client import ModbusTcpClient
    from pymodbus.exceptions import ModbusException
except ImportError:
    modbus_installed_ = False
    ModbusTcpClient = None  # type: ignore
    ModbusException = Exception  # type: ignore
else:
    modbus_installed_ = True

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.process_log as process_log
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.utils_columns import get_current_utc_time

# Default Modbus unit id on TCP
DEFAULT_MODBUS_UNIT = 1


@dataclass
class ModbusReadContext:
    """PDU unit, map decode options, labels, and TCP endpoint for ``read_data`` (no PLC streaming fields)."""

    unit: int = DEFAULT_MODBUS_UNIT
    map_options: dict | None = None
    field_labels: dict | None = None
    tcp_url: str | None = None


def _modbus_dynamic_segment(s: str) -> str:
    """One path/table segment: safe chars, no leading digit, non-empty (dynamic table names and UNS leaves)."""
    t = re.sub(r"[^0-9A-Za-z_]", "_", str(s))
    if t and t[0].isdigit():
        t = "t_" + t
    return (t or "x")[:120]


def modbus_dynamic_table_name(client_name: str, map_field_name: str) -> str:
    """``dynamic = true`` without UNS: one table per map ``name`` → ``{client_name}_{map_field}`` (sanitized)."""
    return f"{_modbus_dynamic_segment(client_name)}_{_modbus_dynamic_segment(map_field_name)}"[:200]


class ModbusUnsStreamContext:
    """
    Holds the in-memory UNS adapter and blockchain insert targets for Modbus ``dynamic`` + ``namespace``.

    ``adapter`` is ``mqtt_client.SubscriptionInfo``: that class lives in the message-client module for historical
    reasons, but here it is used only as the **dynamic topic registry** (synthetic path → dbms/table, mutex, mem
    view for ``blockchain_insert_all``)—not for MQTT broker I/O. Modbus TCP does not use the MQTT protocol.
    """

    __slots__ = ("adapter", "master_node", "platform", "base_namespace")

    def __init__(self, adapter, master_node, platform, base_namespace):
        self.adapter = adapter
        self.master_node = master_node
        self.platform = platform
        self.base_namespace = base_namespace


def modbus_plc_resolve_dynamic_uns_context(status, plc_type, dbms_name, dynamic_tables, ns_trim, conditions):
    """
    If ``namespace`` is set with Modbus ``dynamic = true``, build UNS context (else return ``(SUCCESS, None)``).

    Reuses ``SubscriptionInfo`` from ``edge_lake.tcpip.mqtt_client`` so ``manage_uns.add_uns_policies`` and
    ``get_dbms_table`` / ``add_new_topic`` behave the same as the MQTT dynamic path. That reuse is **registry-only**;
    no MQTT connection or publish/subscribe is involved for Modbus.

    The import is lazy (inside this function) and aliased as ``mqtt_mod`` to avoid a module-level import cycle:
    ``mqtt_client`` → ``member_cmd`` → ``plc_client`` → ``modbus_client``.
    """
    # ``mqtt_client``: reused from the MQTT message-client code (``SubscriptionInfo``, dynamic UNS).
    from edge_lake.tcpip import mqtt_client as mqtt_mod

    ns = (ns_trim or "").strip()
    if ns and not dynamic_tables:
        status.add_error("Run PLC client: namespace = ... requires dynamic = true")
        return process_status.ERR_command_struct, None
    if ns and plc_type != "modbus":
        status.add_error("Run PLC client: namespace = ... is only supported for type = modbus")
        return process_status.ERR_command_struct, None
    if not (bool(dynamic_tables) and plc_type == "modbus" and ns):
        return process_status.SUCCESS, None
        
    mn_u = conditions.get("master_node", None)
    if not mn_u:
        status.add_error(
            "Run PLC client (modbus): namespace = ... for UNS policies requires master_node = [ip:port]"
        )
        return process_status.ERR_command_struct, None
    # Path is exactly ``namespace =`` from the command (same as git stash ``modbus_plc_resolve_dynamic_uns_context``).
    base_ns = ns.rstrip("/")
    # Trailing ``…/#``: same prefix-wildcard ``SubscriptionInfo`` (``mqtt_client``) uses for dynamic UNS, so
    # ``add_new_topic("base/leaf")`` matches ``namespace/leaf``.
    wild_key = base_ns + "/#"
    # Same ``SubscriptionInfo`` type the MQTT message client uses for dynamic UNS (topic → dbms/table registry).
    uns_ad = mqtt_mod.SubscriptionInfo()
    uns_ad.set_mem_view()
    uns_mu = threading.Lock()
    uns_ad.add_topic(wild_key, dbms_name, None, 0, [], None, False, True, uns_mu, {})

    return process_status.SUCCESS, ModbusUnsStreamContext(uns_ad, mn_u, None, base_ns)


def modbus_device_id_specified(conditions: dict) -> bool:
    """True if ``device_id`` was set on the command (Modbus PDU unit address)."""
    return "device_id" in conditions


def read_modbus_device_id(conditions: dict, default: int = DEFAULT_MODBUS_UNIT) -> int:
    """
    Modbus PDU unit address (commonly 0–247) from command keyword ``device_id``.
    If ``device_id`` is absent, returns ``default`` (used only for discovery fallbacks).
    """
    if "device_id" in conditions:
        return int(interpreter.get_one_value(conditions, "device_id"))
    return int(default)


_pymodbus_loggers = (
    "pymodbus",
    "pymodbus.client",
    "pymodbus.client.tcp",
    "pymodbus.client.sync",
    "pymodbus.client.base",
    "pymodbus.transport",
    "pymodbus.logging",
    "pymodbus.transaction",
    "pymodbus.framer",
)


def _modbus_client_label(client=None, client_name=None, host=None, port=None):
    """Build a label for logs: 'Modbus TCP (name=..., endpoint=host:port)'."""
    parts = []
    if client is not None:
        if client_name is None:
            client_name = getattr(client, "_anylog_client_name", None)
        if host is None:
            host = getattr(client, "_anylog_host", None)
        if port is None:
            port = getattr(client, "_anylog_port", None)
    name = client_name
    if name:
        parts.append(f"name={name}")
    if host:
        if port is None:
            parts.append(f"host={host}")
        else:
            parts.append(f"endpoint={host}:{port}")
    if not parts:
        return "Modbus TCP"
    return "Modbus TCP (" + ", ".join(parts) + ")"


def _log_modbus_read_error(status, client, where: str, err, accumulator=None):
    """Record one Modbus read failure (or accumulate it for one-line summary)."""
    err_text = str(err)
    if accumulator is not None:
        accumulator.append((where, err_text))
        return
    status.add_error(f"{_modbus_client_label(client)}: read failure {where}: {err_text}")


def _log_modbus_read_critical(client, where: str, err):
    """Record decode/cast failures as CRITICAL-tagged entries in AnyLog Error log."""
    process_log.add(
        "Error",
        f"CRITICAL {_modbus_client_label(client)}: read decode failure {where}: {err}",
    )


def _set_pymodbus_console_loggers():
    """
    Silence pymodbus ERROR/WARNING console output; AnyLog logs read/connect errors directly.
    """
    if not modbus_installed_:
        return

    logger_names = set(_pymodbus_loggers)
    mgr = getattr(logging.root, "manager", None)
    if mgr is not None:
        for name in list(mgr.loggerDict.keys()):
            if isinstance(name, str) and name.startswith("pymodbus"):
                logger_names.add(name)

    for name in logger_names:
        lg = logging.getLogger(name)
        lg.setLevel(logging.CRITICAL)
        lg.propagate = False
        lg.handlers = []

# Standard Modbus per-request limits (PDU): 125 registers, 2000 bits.
_MAX_HOLDING_OR_INPUT = 125
_MAX_COILS_OR_DISCRETE = 2000
_DEFAULT_DISCOVERY_MAX_REGISTERS = 50
_DEFAULT_DISCOVERY_SCAN_CHUNK = 10
_MAX_MODBUS_ADDRESS_SPACE = 65536

# Placeholders for ``get modbus struct`` when format = run_client (user may override via keywords).
_DEFAULT_MODBUS_STRUCT_CLIENT_NAME = "modbus_client"
_DEFAULT_MODBUS_STRUCT_FREQUENCY = 1
_DEFAULT_MODBUS_STRUCT_DBMS = "my_dbms"
_DEFAULT_MODBUS_STRUCT_TABLE = "modbus_readings"


def test_lib_installed(status):
    """Return SUCCESS when pymodbus is available; otherwise log and return import failure."""
    if modbus_installed_:
        return process_status.SUCCESS
    status.add_error("Lib pymodbus for Modbus TCP not installed")
    return process_status.Failed_to_import_lib


def _parse_host_port(url: str):
    """
    url is host:port or host (standard Modbus TCP service port when port is omitted).
    IPv6 is not supported in this shorthand; use host:port with bracketed IPv6 if needed later.
    """
    url = (url or "").strip()
    if not url:
        return None, None
    if ":" in url:
        host, _, port_s = url.rpartition(":")
        host = host.strip()
        try:
            port = int(port_s.strip())
        except ValueError:
            return None, None
        return host, port
    return url, 502


def declare_connection(status, url: str, user=None, password=None, timeout_sec=None, client_name=None):
    """
    Open Modbus TCP. user/password are reserved for future TLS or serial gateway auth.
    timeout_sec: socket timeout in seconds (default 5, max 30).
    """
    if not modbus_installed_:
        status.add_error("Modbus: pymodbus not installed")
        return None
    _set_pymodbus_console_loggers()
    host, port = _parse_host_port(url)  # Commands pass host:port in url for this client.
    if not host:
        status.add_error(
            "Modbus: invalid host:port in url\n"
            "→ Logged to: AnyLog Error log"
        )
        return None
    try:
        to = 5.0 if timeout_sec is None else float(timeout_sec)
        # Bound connect timeout so CLI commands fail fast but can still reach slow PLCs.
        if to < 1.0:
            to = 1.0
        elif to > 30.0:
            to = 30.0
    except (TypeError, ValueError):
        to = 5.0
    try:
        client = ModbusTcpClient(host=host, port=port, timeout=to)
        client._anylog_client_name = client_name
        client._anylog_host = host
        client._anylog_port = port
        # ModbusTcpClient() attaches loggers after our first _set_pymodbus_console_loggers() call.
        _set_pymodbus_console_loggers()
        if not client.connect():  # pymodbus: opens TCP and optionally probes unit.
            error_msg = (
                f"{_modbus_client_label(client_name=client_name, host=host, port=port)}: "
                f"TCP connect failed (timeout={to}s)"
            )
            status.add_error(error_msg)
            utils_print.output_box(error_msg)
            return None
    except Exception:
        _exc = sys.exc_info()[1]
        error_msg = (
            f"{_modbus_client_label(client_name=client_name, host=host, port=port)}: "
            f"TCP connect exception (timeout={to}s): {_exc}"
        )
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        return None
    return client


def disconnect_modbus(status, client, url: str):
    """Close a Modbus TCP client and report disconnect errors through status."""
    if client is None:
        return True
    host, port = _parse_host_port(url)
    try:
        client.close()
    except Exception:
        _exc = sys.exc_info()[1]
        status.add_error(
            f"{_modbus_client_label(client, host=host, port=port)}: disconnect error: {_exc}"
        )
        return False
    return True


def _modbus_try_reopen_tcp(status, client, url: str) -> bool:
    """
    After transport-level read failures (stale socket, RST, idle drop), close the pymodbus client and
    ``connect()`` again on the **same** instance so callers (``run_plc_client`` / ``process_data``) keep a
    valid reference without threading a new client object through return values.
    """
    if client is None:
        return False
    try:
        client.close()
    except Exception:
        pass
    try:
        _set_pymodbus_console_loggers()
        if client.connect():
            return True
        status.add_error(
            f"Modbus: TCP reconnect failed after transport read errors (endpoint={url})"
        )
        return False
    except Exception:
        _exc = sys.exc_info()[1]
        status.add_error(
            f"Modbus: TCP reconnect exception after transport read errors (endpoint={url}): {_exc}"
        )
        return False


def modbus_error_looks_transport(detail: str | None) -> bool:
    """
    Heuristic: Modbus read failure is likely TCP/transport (device off, cable, firewall) rather than map/decode.
    Used to decide whether to reopen the TCP session and retry reads once.
    """
    if not detail:
        return False
    d = str(detail).lower()
    needles = (
        "connection",
        "connect",
        "timeout",
        "timed out",
        "refused",
        "reset",
        "broken pipe",
        "no route",
        "unreachable",
        "network",
        "socket",
        "errno",
        "modbusio",
        "modbus io",
        "partial response",
        "no response",
        "bad file descriptor",
        "host is down",
        "input/output",
    )
    return any(n in d for n in needles)


def _extract_values(resp, val_attr: str, requested_count: int):
    """
    Normalize pymodbus response payload to a list and trim padded bit arrays.
    Used by both bit and register read paths.
    """
    vals = getattr(resp, val_attr, None)
    if vals is None:
        return []
    if val_attr == "bits":
        # Some servers/libraries pad to a full byte; keep only requested addresses.
        return list(vals)[:requested_count]
    return list(vals)


def _format_modbus_error(err_obj) -> str:
    """Human-readable Modbus error text for logs/tables."""
    exc_code = getattr(err_obj, "exception_code", None)
    if exc_code is not None:
        code_map = {
            1: "illegal function",
            2: "illegal data address",
            3: "illegal data value",
            4: "server device failure",
            5: "acknowledge",
            6: "server device busy",
            8: "memory parity error",
            10: "gateway path unavailable",
            11: "gateway target device failed to respond",
        }
        fc = getattr(err_obj, "function_code", None)
        if isinstance(fc, int) and fc >= 0x80:
            fc -= 0x80
        fc_map = {
            1: "read coils",
            2: "read discrete inputs",
            3: "read holding registers",
            4: "read input registers",
        }
        fc_txt = fc_map.get(fc, f"function code {fc}" if fc is not None else "function code ?")
        return f"{fc_txt}: {code_map.get(int(exc_code), 'modbus exception')} (code={int(exc_code)})"
    return str(getattr(err_obj, "message", err_obj))


def _scan_holding_or_input(unit: int, max_addr: int, chunk: int, read_fn, prefix: str):
    """Scan 0..max_addr-1; on illegal address skip chunk. Returns list of tag ids (prefix:addr)."""
    tags = []
    addr = 0
    while addr < max_addr:
        count = min(chunk, max_addr - addr)
        try:
            resp = read_fn(addr, count=count, device_id=unit)
        except (ModbusException, OSError, ConnectionError) as ex:
            process_log.add("Error", f"Modbus: scan {prefix} at {addr}: {ex}")
            addr += count
            continue
        except Exception:
            _exc = sys.exc_info()[1]
            process_log.add("Error", f"Modbus: scan {prefix} at {addr}: {_exc}")
            addr += count
            continue
        if resp.isError():
            addr += count  # No readable registers in this chunk; keep scanning.
            continue
        regs = getattr(resp, "registers", None)
        if regs is None:
            addr += count
            continue
        for i in range(len(regs)):
            tags.append(f"{prefix}:{addr + i}")
        addr += count
    return tags


def _scan_bits(unit: int, max_addr: int, chunk: int, read_fn, prefix: str):
    """Scan bit addresses 0..max_addr-1 in chunks and return discovered tags (prefix:addr)."""
    tags = []
    addr = 0
    while addr < max_addr:
        count = min(chunk, max_addr - addr)
        try:
            resp = read_fn(addr, count=count, device_id=unit)
        except (ModbusException, OSError, ConnectionError) as ex:
            process_log.add("Error", f"Modbus: scan {prefix} at {addr}: {ex}")
            addr += count
            continue
        except Exception:
            _exc = sys.exc_info()[1]
            process_log.add("Error", f"Modbus: scan {prefix} at {addr}: {_exc}")
            addr += count
            continue
        if resp.isError():
            addr += count  # No readable bits in this chunk; keep scanning.
            continue
        bits = getattr(resp, "bits", None)
        if bits is None:
            addr += count
            continue
        for i in range(len(bits)):
            tags.append(f"{prefix}:{addr + i}")
        addr += count
    return tags


def expand_modbus_register_map(status, map_raw):
    """
    Build point list from a JSON register map.
    Supported syntax:
    [
      {"name":"temperature","register":[10,11],"type":"float","swap":"words"},
      {"name":"active","coil":1}
    ]
    With ``dynamic = true`` on ``run plc client`` (omit ``table =``), streaming uses one table per map row:
    table name is ``name =`` from the command (client id) plus underscore plus that row's JSON ``name`` (e.g. ``mydev_1_door_1``).
    A point value may be a single int address, or a list of consecutive int addresses (multi-read).

    Return value is a list of four items:
    - ret_val: status code (success or error).
    - tags_list: internal point ids in read order, e.g. ``hr:10``, ``c:0``.
    - labels: for each internal id, the matching ``name`` string from the map JSON (used as column label / dynamic table suffix).
    - options: extra decode settings — ``per_field`` (type, swap, scale, … per ``name``) and ``field_meta`` (source kind, register count, …).
    """
    if map_raw is None or (isinstance(map_raw, str) and not str(map_raw).strip()):
        status.add_error("Modbus: empty map")
        return [process_status.ERR_process_failure, None, None, None]

    if isinstance(map_raw, str):
        parsed = utils_json.str_to_json(map_raw)
        if parsed is None:
            status.add_error("Modbus: map is not valid JSON")
            return [process_status.ERR_command_struct, None, None, None]
    else:
        parsed = map_raw
    if isinstance(parsed, list):
        parsed = {"values": parsed}

    if not isinstance(parsed, dict):
        status.add_error("Modbus: map must be a JSON array (or object with key 'values')")
        return [process_status.ERR_command_struct, None, None, None]

    # Do not mutate caller dict
    parsed = dict(parsed)
    modbus_map_options = {"per_field": {}, "field_meta": {}}

    labels = {}
    tags = []

    def _one_address(prefix: str, name, addr, is_bits: bool) -> str | None:
        """Build canonical tag for one point; returns None on error (after status.add_error)."""
        if isinstance(addr, (int, float)) and not isinstance(addr, bool):
            try:
                a = int(addr)
            except (TypeError, ValueError):
                status.add_error(
                    f"Modbus: map['{name}'] address must be an integer, list of integers, or consecutive addresses"
                )
                return None
            return f"{prefix}:{a}"
        if isinstance(addr, list):
            if not addr:
                status.add_error(f"Modbus: map field '{name}' has empty address list")
                return None
            if not all(
                isinstance(x, (int, float)) and not isinstance(x, bool) for x in addr
            ):
                status.add_error(
                    f"Modbus: map['{name}'] address list must contain only integers (consecutive)"
                )
                return None
            int_addrs = [int(x) for x in addr]
            n = len(int_addrs)
            if n == 1:
                return f"{prefix}:{int_addrs[0]}"
            for i in range(1, n):
                if int_addrs[i] != int_addrs[i - 1] + 1:
                    status.add_error(
                        f"Modbus: map field '{name}': non-consecutive address list; only contiguous blocks are supported"
                    )
                    return None
            start, count = int_addrs[0], n
            if count > 1 and is_bits and count > _MAX_COILS_OR_DISCRETE:
                status.add_error(
                    f"Modbus: map field '{name}': requested bit count {count} exceeds a safe single read ({_MAX_COILS_OR_DISCRETE})"
                )
                return None
            if count > 1 and (not is_bits) and count > _MAX_HOLDING_OR_INPUT:
                status.add_error(
                    f"Modbus: map field '{name}': requested register count {count} exceeds the Modbus per-request cap ({_MAX_HOLDING_OR_INPUT})"
                )
                return None
            return f"{prefix}:{start}+{count}"
        status.add_error(
            f"Modbus: map['{name}'] address must be an integer or a list of consecutive integer addresses"
        )
        return None

    def _item_type_as_str(item, field_name: str):
        """Read optional item 'type' and normalize to lowercase string; returns '__err__' on invalid value."""
        tv = item.get("type", None)
        if tv is None:
            return None
        if not isinstance(tv, str) or not tv.strip():
            status.add_error(f"Modbus: map item '{field_name}': 'type' must be a non-empty string")
            return "__err__"
        return tv.strip().lower()

    def _item_swap_as_str(item, field_name: str):
        """Read optional item 'swap' and validate allowed values; returns '__err__' on invalid value."""
        sv = item.get("swap", None)
        if sv is None:
            return None
        if not isinstance(sv, str) or not sv.strip():
            status.add_error(
                f"Modbus: map item '{field_name}': 'swap' must be a string: bytes|words|both|none"
            )
            return "__err__"
        sv = sv.strip().lower()
        if sv not in ("bytes", "words", "both", "none"):
            status.add_error(
                f"Modbus: map item '{field_name}': unsupported swap '{sv}' (allowed: bytes, words, both, none)"
            )
            return "__err__"
        return sv

    def _item_number(item, field_name: str, k: str):
        """Read optional numeric modifier (scale/offset) and return float; returns '__err__' on invalid value."""
        v = item.get(k, None)
        if v is None:
            return None
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            status.add_error(
                f"Modbus: The value of {k} for item '{field_name}' in the modbus map should be a floating point number"
            )
            return "__err__"
        return float(v)

    def _addr_count(addr) -> int | None:
        """Return register count for int/list addresses; None when address format is unsupported."""
        if isinstance(addr, (int, float)) and not isinstance(addr, bool):
            return 1
        if isinstance(addr, list):
            return len(addr)
        return None

    values_list = parsed.get("values", None)
    if values_list is None:
        status.add_error("Modbus: map must be a JSON array (or object with key 'values')")
        return [process_status.ERR_command_struct, None, None, None]
    if not isinstance(values_list, list):
        status.add_error("Modbus: map['values'] must be a list")
        return [process_status.ERR_command_struct, None, None, None]

    for i, item in enumerate(values_list):
        if not isinstance(item, dict):
            status.add_error(f"Modbus: map['values'][{i}] must be an object")
            return [process_status.ERR_command_struct, None, None, None]
        nm = item.get("name", None)
        if not isinstance(nm, str) or not nm.strip():
            status.add_error(f"Modbus: map['values'][{i}] is missing non-empty 'name'")
            return [process_status.ERR_command_struct, None, None, None]
        field_name = nm.strip()

        source_keys = [k for k in ("register", "inputRegister", "coil", "input") if k in item]
        if len(source_keys) != 1:
            status.add_error(
                f"Modbus: map item '{field_name}' must include exactly one of register, inputRegister, coil, input"
            )
            return [process_status.ERR_command_struct, None, None, None]

        source = source_keys[0]
        if source == "register":
            pref = "hr"
        elif source == "inputRegister":
            pref = "ir"
        elif source == "coil":
            pref = "c"
        elif source == "input":
            pref = "di"
        else:
            status.add_error(
                f"Modbus: map item '{field_name}': unsupported source key '{source}'"
            )
            return [process_status.ERR_command_struct, None, None, None]

        tag = _one_address(pref, field_name, item.get(source), source in ("coil", "input"))
        if tag is None:
            return [process_status.ERR_command_struct, None, None, None]
        if tag not in labels:
            tags.append(tag)
        labels[tag] = field_name

        f_type = _item_type_as_str(item, field_name)
        if f_type == "__err__":
            return [process_status.ERR_command_struct, None, None, None]
        f_swap = _item_swap_as_str(item, field_name)
        if f_swap == "__err__":
            return [process_status.ERR_command_struct, None, None, None]
        f_scale = _item_number(item, field_name, "scale")
        if f_scale == "__err__":
            return [process_status.ERR_command_struct, None, None, None]
        f_offset = _item_number(item, field_name, "offset")
        if f_offset == "__err__":
            return [process_status.ERR_command_struct, None, None, None]

        if source in ("coil", "input"):
            if any(v is not None for v in (f_type, f_swap, f_scale, f_offset)):
                status.add_error(
                    f"Modbus: map item '{field_name}': coil/input do not support modifiers"
                )
                return [process_status.ERR_command_struct, None, None, None]
        elif source in ("register", "inputRegister"):
            if f_type is not None and f_type not in ("long", "float", "byte"):
                status.add_error(
                    f"Modbus: map item '{field_name}': type supports only 'long', 'float', or 'byte'"
                )
                return [process_status.ERR_command_struct, None, None, None]
            if f_type == "long":
                count = _addr_count(item.get(source))
                if count is None or count < 1 or count > 4:
                    status.add_error(
                        f"Modbus: map item '{field_name}': type '{f_type}' requires 1 to 4 consecutive registers"
                    )
                    return [process_status.ERR_command_struct, None, None, None]
            if f_type == "byte":
                count = _addr_count(item.get(source))
                if count is None or count <= 4:
                    status.add_error(
                        f"Modbus: map item '{field_name}': type 'byte' requires more than 4 consecutive registers"
                    )
                    return [process_status.ERR_command_struct, None, None, None]

        field_cfg = {}
        if f_type is not None:
            field_cfg["type"] = f_type
        if f_swap is not None:
            field_cfg["swap"] = f_swap
        if f_scale is not None:
            field_cfg["scale"] = f_scale
        if f_offset is not None:
            field_cfg["offset"] = f_offset
        if field_cfg:
            modbus_map_options["per_field"][field_name] = field_cfg
        modbus_map_options["field_meta"][field_name] = {
            "source": source,
            "register_count": _addr_count(item.get(source)),
        }

    if not tags:
        status.add_error("Modbus: map['values'] has no entries")
        return [process_status.ERR_process_failure, None, None, None]

    return [process_status.SUCCESS, tags, labels, modbus_map_options]


def modbus_dynamic_publish_row(status, plc_label, dbms_name, plc_client_name, modbus_field_labels, entry, timestamp_first, attr_val, prep_dir, watch_dir, err_dir, uns_ctx: ModbusUnsStreamContext | None = None):
    """
    ``run plc client`` with ``dynamic = true``: one map point per row.
    Without ``namespace``: JSON ``tag`` + ``value`` + ``timestamp`` to table ``modbus_dynamic_table_name(client, map name)``.
    With UNS (``uns_ctx``): JSON uses the map ``name`` as the value column (e.g. ``coil_0``) plus ``timestamp``—no separate generic ``value`` key.
    Returns ``(ret_val, inserted)`` for the PLC read loop.
    """
    map_field = (modbus_field_labels or {}).get(entry.tag)
    if not map_field:
        status.add_error(
            f"{plc_label}: dynamic mode expects each map point to have a JSON 'name'; "
            f"missing label for internal tag {entry.tag!r}"
        )
        return process_status.ERR_process_failure, False

    if uns_ctx is not None:
        return _modbus_dynamic_publish_uns(status, plc_label, uns_ctx, plc_client_name, map_field, attr_val, timestamp_first, prep_dir, watch_dir, err_dir)

    # Dynamic table: ``name =`` from run plc client + map ``name`` (sanitized segments).
    target_table = modbus_dynamic_table_name(plc_client_name, map_field)
    json_row = {}
    if timestamp_first:
        try:
            if isinstance(timestamp_first, str):
                json_row["timestamp"] = timestamp_first
            else:
                json_row["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            pass
    if "timestamp" not in json_row:
        err_msg = f"{plc_label}: failed to set a timestamp attribute using: '{timestamp_first}'"
        status.add_error(err_msg)
        utils_print.output_box(err_msg)
        return process_status.ERR_process_failure, False

    json_row["tag"] = map_field
    json_row["value"] = attr_val
    json_msg = utils_json.to_string(json_row)
    if not json_msg:
        status.add_error(f"{plc_label}: failed to process JSON row: '{str(json_row)}'")
        return process_status.ERR_process_failure, False

    # add_data: mode streaming; 1 = row count for this payload; "0"/"0" = default source & instructions (stream buffer key)
    ret_val, _hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, dbms_name, target_table, "0", "0", "json", json_msg)
    if ret_val:
        return ret_val, False
    return process_status.SUCCESS, True


def _modbus_dynamic_publish_uns(status, plc_label, uns_ctx: ModbusUnsStreamContext, client_slug: str, map_field: str, attr_val, timestamp_first, prep_dir, watch_dir, err_dir):
    """
    UNS branch for :func:`modbus_dynamic_publish_row`.

    Payload is the map ``name`` as the sole data key (e.g. ``coil_0``) plus ``timestamp``—no generic ``value`` key,
    so the stored column matches the map field name. ``uns_ctx.adapter`` is ``SubscriptionInfo`` (registry only).
    """
    from anylog_enterprise.uns import manage_uns as _manage_uns

    fk = str(map_field).strip() if map_field is not None else ""
    if not fk:
        status.add_error(f"{plc_label}: UNS dynamic row needs a non-empty map JSON 'name'")
        return process_status.ERR_process_failure, False
    if fk.lower() == "timestamp":
        status.add_error(
            f"{plc_label}: map JSON 'name' must not be 'timestamp' (reserved); got {map_field!r}"
        )
        return process_status.ERR_process_failure, False

    # Single reading column: map JSON ``name`` only. Do not add a generic ``value`` key—that duplicated the same
    # scalar in two DB columns (``value`` + ``coil_0``) and broke UNS table shape vs non-UNS Modbus.
    json_one = {fk: attr_val}
    try:
        if isinstance(timestamp_first, str):
            json_one["timestamp"] = timestamp_first
        else:
            json_one["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        json_one["timestamp"] = str(timestamp_first)
    json_msg = utils_json.to_string(json_one)
    if not json_msg:
        status.add_error(f"{plc_label}: failed to process JSON row for field '{map_field}'")
        return process_status.ERR_process_failure, False

    slug = (client_slug or "").strip()
    # Match stash / dynamic UNS: leaf = {name}_{map field} when ``name =`` is set; namespace is only the path prefix.
    # Synthetic topic ``{namespace}/{leaf}`` e.g. FACTORY/MIDDLE/DEVICE/dynuns4_door_1 → table ``dynuns4_door_1_1`` from UNS.
    if slug:
        leaf = modbus_dynamic_table_name(slug, map_field)
    else:
        base_ns = (uns_ctx.base_namespace or "").rstrip("/").strip()
        segs = [p for p in base_ns.split("/") if p]
        ns_last = segs[-1] if segs else ""
        if ns_last:
            leaf = modbus_dynamic_table_name(ns_last, map_field)
        else:
            leaf = _modbus_dynamic_segment(str(map_field).strip() if map_field is not None else "")
    synthetic = f"{uns_ctx.base_namespace}/{leaf}".replace("//", "/")

    adapter = uns_ctx.adapter  # SubscriptionInfo: synthetic path → dbms/table after add_uns_policies
    if not adapter.get_topic_info(synthetic):
        if adapter.add_new_topic(synthetic):
            ret_u = _manage_uns.add_uns_policies(
                status,
                uns_ctx.master_node,
                uns_ctx.platform,
                adapter,
                synthetic,
            )
            if ret_u:
                return ret_u, False
    pair = adapter.get_dbms_table(synthetic)
    if not pair or not pair[1]:
        status.add_error(
            f"{plc_label}: UNS did not assign a table for '{synthetic}' (check master_node and UNS policies)"
        )
        return process_status.ERR_process_failure, False

    # ``get_dbms_table`` returns ``(dbms_name, table_name)`` for the synthetic UNS path—``pair[0]`` is DBMS, ``pair[1]`` is table.
    stream_dbms, stream_table = pair[0], pair[1]
    if slug and stream_table:
        pfx = _modbus_dynamic_segment(slug)
        tnorm = str(stream_table).lower()
        plow = pfx.lower()
        if plow and not (tnorm == plow or tnorm.startswith(plow + "_")):
            stream_table = utils_data.reset_str_chars(f"{pfx}_{stream_table}")
            adapter.update_table(synthetic, stream_table)

    # add_data: ``1`` = row count for this payload; ``"0"`` / ``"0"`` = default source and instructions (stream buffer key).
    ret_val, _hash_value = add_data(
        status, "streaming", 1, prep_dir, watch_dir, err_dir, stream_dbms, stream_table, "0", "0", "json", json_msg
    )
    if ret_val:
        return ret_val, False

    return process_status.SUCCESS, True


def discover_all_points(status, client, conditions: dict):
    """
    Probe the Modbus device in blocks; collect addresses that return valid data for
    holding registers, input registers, coils, and discrete inputs.
    """
    unit = read_modbus_device_id(conditions, DEFAULT_MODBUS_UNIT)
    max_addr = int(
        interpreter.get_one_value_or_default(
            conditions, "max_registers", _DEFAULT_DISCOVERY_MAX_REGISTERS
        )
    )
    chunk = int(
        interpreter.get_one_value_or_default(
            conditions, "scan_chunk", _DEFAULT_DISCOVERY_SCAN_CHUNK
        )
    )
    max_addr = max(1, min(max_addr, _MAX_MODBUS_ADDRESS_SPACE))
    chunk = max(1, min(chunk, _MAX_HOLDING_OR_INPUT))

    # Four independent scans (hr/ir/c/di); failures in one kind do not stop the others.
    tags = []
    tags += _scan_holding_or_input(
        unit, max_addr, chunk, client.read_holding_registers, "hr"
    )
    tags += _scan_holding_or_input(
        unit, max_addr, chunk, client.read_input_registers, "ir"
    )
    coil_chunk = min(chunk, _MAX_COILS_OR_DISCRETE)
    tags += _scan_bits(unit, max_addr, coil_chunk, client.read_coils, "c")
    tags += _scan_bits(
        unit, max_addr, coil_chunk, client.read_discrete_inputs, "di"
    )

    if not tags:
        status.add_error(
            "Modbus: discovery found no readable addresses in the configured range "
            f"(device_id={unit}, max_registers={max_addr}). Increase max_registers or set explicit nodes."
        )
    return tags


def modbus_struct(status, io_buff_in, cmd_words, trace):
    """
    Discover readable Modbus points and output:
    - format = nodes      -> list of canonical tags (hr:/ir:/c:/di:)
    - format = map        -> map array syntax for get/run commands (default)
    - format = get_value  -> ready-to-run get modbus values command
    - format = run_client -> ready-to-run run plc client where type = modbus ... command
    """
    keywords = {
        "hostname": ("str", True, False, True),
        "port": ("int", False, False, True),
        "device_id": ("int", True, False, True),
        "max_registers": ("int", False, False, True),
        "scan_chunk": ("int", False, False, True),
        "format": ("str", False, False, True),
        "name": ("str", False, False, True),
        "frequency": ("int.frequency", False, False, False),
        "dbms": ("str", False, True, True),
        "table": ("str", False, True, True),
    }
    ret_val, _counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    host_val = interpreter.get_one_value_or_default(conditions, "hostname", None)
    if not host_val:
        status.add_error("Get modbus struct: specify hostname")
        return [process_status.ERR_command_struct, None]
    port_val = int(interpreter.get_one_value_or_default(conditions, "port", 502))
    url = f"{host_val}:{port_val}"
    conditions["url"] = [url]

    if not modbus_device_id_specified(conditions):
        status.add_error("Get modbus struct: device_id is required")
        return [process_status.ERR_command_struct, None]

    output_format = interpreter.get_one_value_or_default(conditions, "format", "map")
    valid_formats = ["nodes", "map", "get_value", "run_client"]
    if output_format not in valid_formats:
        status.add_error("Modbus: invalid output format, expected values are: " + ", ".join(valid_formats))
        return [process_status.ERR_command_struct, None]

    ret_val = test_lib_installed(status)
    if ret_val:
        return [ret_val, None]

    client = declare_connection(status, url, None, None)
    if not client:
        return [process_status.Failed_PLC_CONNECT, None]

    tags = discover_all_points(status, client, conditions)
    disconnect_modbus(status, client, url)
    if not tags:
        return [process_status.ERR_process_failure, None]

    tags = sorted(set(tags), key=lambda t: (t.split(":", 1)[0], t))
    if output_format == "nodes":
        return [process_status.SUCCESS, utils_json.to_string(tags)]

    # Map item ``name`` tokens (hr_, ir_, c_, di_) match canonical tag prefixes from
    # discovery / _tag_str / _parse_tag so generated maps read consistently with ``nodes``.
    map_items = []
    for tag in tags:
        kind, start, count = _parse_tag(tag)
        if kind is None:
            continue
        if kind == "holding":
            source_key = "register"
            name_prefix = "hr"
        elif kind == "input":
            source_key = "inputRegister"
            name_prefix = "ir"
        elif kind == "coil":
            source_key = "coil"
            name_prefix = "c"
        else:
            source_key = "input"
            name_prefix = "di"

        if count == 1:
            source_val = start
            name = f"{name_prefix}_{start}"
        else:
            source_val = [int(x) for x in range(start, start + count)]
            name = f"{name_prefix}_{start}_{start + count - 1}"

        map_items.append({"name": name, source_key: source_val})
    map_json = utils_json.to_string(map_items)
    if output_format == "map":
        return [process_status.SUCCESS, map_json]

    if output_format == "get_value":
        cmd = (
            "<get modbus values where "
            f"hostname = {host_val} and port = {port_val} and device_id = {read_modbus_device_id(conditions)} "
            f"and map = {map_json}>"
        )
        return [process_status.SUCCESS, cmd]

    client_name = interpreter.get_one_value_or_default(
        conditions, "name", _DEFAULT_MODBUS_STRUCT_CLIENT_NAME
    )
    frequency = interpreter.get_one_value_or_default(
        conditions, "frequency", _DEFAULT_MODBUS_STRUCT_FREQUENCY
    )
    dbms_name = interpreter.get_one_value_or_default(
        conditions, "dbms", _DEFAULT_MODBUS_STRUCT_DBMS
    )
    table_name = interpreter.get_one_value_or_default(
        conditions, "table", _DEFAULT_MODBUS_STRUCT_TABLE
    )
    cmd = (
        "<run plc client where type = modbus and "
        f"hostname = {host_val} and port = {port_val} and device_id = {read_modbus_device_id(conditions)} "
        f"and frequency = {frequency} and name = {client_name} and dbms = {dbms_name} and table = {table_name} "
        f"and map = {map_json}>"
    )
    return [process_status.SUCCESS, cmd]


def _parse_tag(tag: str):
    """
    Returns (kind, start_addr, reg_or_bit_count) for hr/ir/c/di tags.
    Count is 1 for hr:7 / c:0; 2+ for hr:4+2 (two holding registers from address 4).
    """
    tag = tag.strip()
    for prefix, kind in (
        ("hr:", "holding"),
        ("ir:", "input"),
        ("c:", "coil"),
        ("di:", "discrete"),
    ):
        if not tag.startswith(prefix):
            continue
        rest = tag[len(prefix) :]
        if not rest:
            return None, None, None
        if "+" in rest:
            a_s, c_s = rest.split("+", 1)
            try:
                start = int(a_s)
                count = int(c_s)
            except (TypeError, ValueError):
                return None, None, None
            if count < 1:
                return None, None, None
            return kind, start, count
        try:
            return kind, int(rest), 1
        except (TypeError, ValueError):
            return None, None, None
    return None, None, None


def _swap16_u16(w: int) -> int:
    """Swap low and high byte of one 16-bit Modbus word (used for swap=bytes / both)."""
    w &= 0xFFFF
    return ((w & 0xFF) << 8) | (w >> 8)


def _swap_words_mode(words: list, mode: str | None):
    """Apply map ``swap`` mode to 16-bit words: none | bytes (per-word) | words (swap pair) | both."""
    out = [int(w) & 0xFFFF for w in words]
    if not mode or mode == "none":
        return out
    if mode in ("bytes", "both"):
        out = [_swap16_u16(w) for w in out]
    if mode in ("words", "both") and len(out) == 2:
        out = [out[1], out[0]]
    return out


def _get_field_cfg(canonical: str, map_options: dict, field_labels: dict | None):
    """
    Resolve per-point map options for one read.

    ``canonical`` is the internal tag (e.g. hr:0+2). ``field_labels`` maps that tag to the
    map item ``name``. ``map_options['per_field'][name]`` holds type/swap/scale/offset for decode.
    """
    cfg = {}
    field_name = field_labels.get(canonical) if field_labels else None
    if (
        field_name
        and map_options
        and isinstance(map_options.get("per_field"), dict)
        and isinstance(map_options["per_field"].get(field_name), dict)
    ):
        cfg.update(map_options["per_field"][field_name])
    return cfg


def _bit_declared_type(canonical: str, map_options: dict, field_labels: dict | None):
    """Map-declared ``type`` for a coil/discrete tag, if present (usually None; coil map items forbid modifiers)."""
    cfg = _get_field_cfg(canonical, map_options, field_labels)
    t = cfg.get("type")
    if isinstance(t, str):
        return t.lower()
    return None


def _batch_sorted_addresses(addresses: list, max_block: int):
    """Merge consecutive addresses into (start, count) reads."""
    if not addresses:
        return []
    addresses = sorted(set(addresses))
    batches = []
    i = 0
    n = len(addresses)
    while i < n:
        start = addresses[i]
        count = 1
        i += 1
        while i < n and addresses[i] == start + count and count < max_block:
            count += 1
            i += 1
        batches.append((start, count))
    return batches


def _tag_str(kind: str, start: int, count: int = 1) -> str:
    """Build canonical Modbus tag string (hr:/ir:/c:/di:) from internal kind + address span."""
    if count < 1:
        count = 1
    if kind == "holding":
        p = "hr"
    elif kind == "input":
        p = "ir"
    elif kind == "coil":
        p = "c"
    else:
        p = "di"
    if count == 1:
        return f"{p}:{start}"
    return f"{p}:{start}+{count}"


def _decode_holding_input_value(
    canonical: str, words: list, map_options: dict, field_labels: dict | None
):
    """
    Decode holding/input register words for supported map types only: long, float, byte, or default uint16.
    ``words`` is one or more 16-bit values from a single Modbus read.
    """
    n = len(words)
    if n < 1:
        return None
    words = [int(x) & 0xFFFF for x in words]
    cfg = _get_field_cfg(canonical, map_options, field_labels)
    words = _swap_words_mode(words, cfg.get("swap", None))

    type_name = cfg.get("type", None)
    if isinstance(type_name, str):
        type_name = type_name.lower()

    if n == 1:
        base = words[0] & 0xFFFF
        if type_name == "long":
            if base & 0x8000:
                base -= 0x10000
            base = int(base)
        elif type_name == "float":
            base = float(base)
        elif type_name == "byte":
            base = str(base & 0xFF)
        else:
            base = int(base)
    else:
        if type_name == "byte":
            base = [w & 0xFFFF for w in words]
        elif type_name == "float" and n == 2:
            total = 0
            for w in words:
                total = (total << 16) | (w & 0xFFFF)
            base = float(
                struct.unpack(">f", int(total).to_bytes(4, byteorder="big", signed=False))[0]
            )
        elif type_name == "long":
            total = 0
            for w in words:
                total = (total << 16) | (w & 0xFFFF)
            bits = n * 16
            if total & (1 << (bits - 1)):
                total -= 1 << bits
            base = int(total)
        else:
            base = [w & 0xFFFF for w in words]

    scale_v = cfg.get("scale", None)
    offset_v = cfg.get("offset", None)
    if isinstance(base, (int, float)) and not isinstance(base, bool):
        if scale_v is not None:
            base = float(base) * float(scale_v)
        if offset_v is not None:
            base = float(base) + float(offset_v)

    return base


def read_data(status, client, tags: list, ctx: ModbusReadContext | None = None, _modbus_reconnect_once: bool = False):
    """
    Read Modbus points; tags are strings hr:n, ir:n, c:n, di:n, optionally hr:n+c for
    a contiguous read of c holding/input registers, or c:n+c / di:n+c for bit blocks.
    ctx: unit, map_options (decode modifiers from map=), field_labels (tag -> map name), tcp_url (host:port
    for reconnect-on-transport-failure). If ctx is None, defaults are used.
    Returns [ret_val, list of SimpleNamespace(tag, value, error)].
    """
    if not tags:
        return [process_status.SUCCESS, []]
    if ctx is None:
        ctx = ModbusReadContext()
    unit = ctx.unit
    map_options = {} if ctx.map_options is None else ctx.map_options
    field_labels = {} if ctx.field_labels is None else ctx.field_labels
    tcp_url = ctx.tcp_url

    # One entry per requested tag string, grouped by table/kind for merged pymodbus reads.
    by_kind: dict = {"holding": [], "input": [], "coil": [], "discrete": []}
    # Accumulate transport/protocol read failures and emit concise ERROR lines once per cycle.
    read_errors = []
    # Dedupe (start, span) ranges per kind so hr:0 and hr:0+2 do not double-read the same FC3 window twice.
    seen_kind_reads: dict = {
        "holding": set(),
        "input": set(),
        "coil": set(),
        "discrete": set(),
    }
    tag_order = []  # (kind, start, span, canonical_tag) in caller order for stable output.
    for t in tags:
        kind, start, span = _parse_tag(t)
        if kind is None:
            status.add_error(
                f"Modbus: invalid tag '{t}' (expected hr:, ir:, c:, or di: with optional +count for multi)"
            )
            return [process_status.Failed_PLC_INFO, None]
        rk = (start, span)
        if rk not in seen_kind_reads[kind]:
            by_kind[kind].append((start, span))
            seen_kind_reads[kind].add(rk)
        tag_order.append((kind, start, span, t))

    # (kind, start, span) -> SimpleNamespace(tag, value, error[, declared_type]); filled below per FC.
    results_map: dict = {}

    def _do_bit_kind(kb: str, read_list: list, read_fn, val_attr, max_block: int):
        """
        Read coils or discrete inputs for this kind (``read_fn`` is FC1 or FC2).
        Singles are merged via ``_batch_sorted_addresses``; each multi-bit tag is one read.
        Values are normalized to int 0/1; ``declared_type`` comes from ``_bit_declared_type`` when map options apply.
        """
        singles, multis = [], []
        for start, span in read_list:
            (singles if span == 1 else multis).append((start, span))
        s_addrs = [s for s, _ in singles]
        for start, count in _batch_sorted_addresses(s_addrs, max_block):
            try:
                resp = read_fn(start, count=count, device_id=unit)
            except Exception:
                _exc = sys.exc_info()[1]
                _log_modbus_read_error(status, client, f"{kb}[{start}..{start + count - 1}]", _exc, read_errors)
                for a in range(start, start + count):
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=_tag_str(kb, a, 1), value=None, error=str(_exc)
                    )
                continue
            if resp.isError():
                err = _format_modbus_error(resp)
                _log_modbus_read_error(status, client, f"{kb}[{start}..{start + count - 1}]", err, read_errors)
                for a in range(start, start + count):
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=_tag_str(kb, a, 1), value=None, error=err
                    )
                continue
            vals = _extract_values(resp, val_attr, count)
            for i, a in enumerate(range(start, start + count)):
                if i < len(vals):
                    v = 1 if bool(vals[i]) else 0
                else:
                    v = None
                can = _tag_str(kb, a, 1)
                results_map[(kb, a, 1)] = SimpleNamespace(
                    tag=can,
                    value=v,
                    error=None,
                    declared_type=_bit_declared_type(can, map_options, field_labels),
                    kind=kb,
                )
        for start, span in multis:
            canonical = _tag_str(kb, start, span)
            try:
                resp = read_fn(start, count=span, device_id=unit)
            except Exception:
                _exc = sys.exc_info()[1]
                _log_modbus_read_error(status, client, canonical, _exc, read_errors)
                results_map[(kb, start, span)] = SimpleNamespace(
                    tag=canonical, value=None, error=str(_exc), kind=kb
                )
                continue
            if resp.isError():
                err = _format_modbus_error(resp)
                _log_modbus_read_error(status, client, canonical, err, read_errors)
                results_map[(kb, start, span)] = SimpleNamespace(
                    tag=canonical, value=None, error=err, kind=kb
                )
                continue
            vals = _extract_values(resp, val_attr, span)
            vlist = [1 if bool(x) else 0 for x in vals[:span]]
            if not vlist:
                v = None
            elif len(vlist) == 1:
                v = vlist[0]
            else:
                v = vlist
            results_map[(kb, start, span)] = SimpleNamespace(
                tag=canonical,
                value=v,
                error=None,
                declared_type=_bit_declared_type(canonical, map_options, field_labels),
                kind=kb,
            )

    def _do_reg_kind(kb: str, read_list: list, read_fn, val_attr, max_block: int):
        """
        Read holding or input registers for this kind (``read_fn`` is FC3 or FC4).
        Singles are merged via ``_batch_sorted_addresses``; each multi-register tag is one read.
        Words are decoded with ``_decode_holding_input_value`` (same path for holding and input).
        """
        singles, multis = [], []
        for start, span in read_list:
            (singles if span == 1 else multis).append((start, span))
        s_addrs = [s for s, _ in singles]
        for start, count in _batch_sorted_addresses(s_addrs, max_block):
            try:
                resp = read_fn(start, count=count, device_id=unit)
            except Exception:
                _exc = sys.exc_info()[1]
                _log_modbus_read_error(status, client, f"{kb}[{start}..{start + count - 1}]", _exc, read_errors)
                for a in range(start, start + count):
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=_tag_str(kb, a, 1), value=None, error=str(_exc)
                    )
                continue
            if resp.isError():
                err = _format_modbus_error(resp)
                _log_modbus_read_error(status, client, f"{kb}[{start}..{start + count - 1}]", err, read_errors)
                for a in range(start, start + count):
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=_tag_str(kb, a, 1), value=None, error=err
                    )
                continue
            vals = _extract_values(resp, val_attr, count)
            for j, a in enumerate(range(start, start + count)):
                if j >= len(vals):
                    can = _tag_str(kb, a, 1)
                    _log_modbus_read_error(status, client, can, "short read", read_errors)
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=can, value=None, error="short read"
                    )
                    continue
                piece = [vals[j]]
                can = _tag_str(kb, a, 1)
                try:
                    wdecode = _decode_holding_input_value(
                        can, piece, map_options, field_labels
                    )
                except Exception as _exc:
                    _log_modbus_read_critical(client, can, _exc)
                    results_map[(kb, a, 1)] = SimpleNamespace(
                        tag=can, value=None, error=f"decode error: {_exc}"
                    )
                    continue
                results_map[(kb, a, 1)] = SimpleNamespace(
                    tag=can, value=wdecode, error=None
                )
        for start, span in multis:
            canonical = _tag_str(kb, start, span)
            try:
                resp = read_fn(start, count=span, device_id=unit)
            except Exception:
                _exc = sys.exc_info()[1]
                _log_modbus_read_error(status, client, canonical, _exc, read_errors)
                results_map[(kb, start, span)] = SimpleNamespace(
                    tag=canonical, value=None, error=str(_exc)
                )
                continue
            if resp.isError():
                err = _format_modbus_error(resp)
                _log_modbus_read_error(status, client, canonical, err, read_errors)
                results_map[(kb, start, span)] = SimpleNamespace(
                    tag=canonical, value=None, error=err
                )
                continue
            vals = _extract_values(resp, val_attr, span)
            try:
                wdecode = _decode_holding_input_value(
                    canonical, list(vals)[:span], map_options, field_labels
                )
            except Exception as _exc:
                _log_modbus_read_critical(client, canonical, _exc)
                results_map[(kb, start, span)] = SimpleNamespace(
                    tag=canonical, value=None, error=f"decode error: {_exc}"
                )
                continue
            results_map[(kb, start, span)] = SimpleNamespace(
                tag=canonical, value=wdecode, error=None
            )

    if by_kind["holding"]:
        _do_reg_kind(
            "holding", by_kind["holding"], client.read_holding_registers, "registers", _MAX_HOLDING_OR_INPUT
        )
    if by_kind["input"]:
        _do_reg_kind(
            "input", by_kind["input"], client.read_input_registers, "registers", _MAX_HOLDING_OR_INPUT
        )
    if by_kind["coil"]:
        _do_bit_kind("coil", by_kind["coil"], client.read_coils, "bits", _MAX_COILS_OR_DISCRETE)
    if by_kind["discrete"]:
        _do_bit_kind(
            "discrete", by_kind["discrete"], client.read_discrete_inputs, "bits", _MAX_COILS_OR_DISCRETE
        )

    # Walk tags in original order; fill gaps with a synthetic error row if a read never populated the key.
    data_points = []
    for kind, start, span, canonical in tag_order:
        key = (kind, start, span)
        entry = results_map.get(key)
        if entry is None:
            _log_modbus_read_error(status, client, canonical, "missing read", read_errors)
            entry = SimpleNamespace(tag=canonical, value=None, error="missing read")
        data_points.append(entry)

    if (
        tcp_url
        and not _modbus_reconnect_once
        and data_points
        and all(getattr(e, "error", None) for e in data_points)
        and any(modbus_error_looks_transport(getattr(e, "error", None)) for e in data_points)
    ):
        if _modbus_try_reopen_tcp(status, client, tcp_url):
            return read_data(
                status,
                client,
                tags,
                ModbusReadContext(unit=unit, map_options=map_options, field_labels=field_labels, tcp_url=tcp_url),
                True,
            )

    if read_errors:
        grouped = {}
        for where, err_text in read_errors:
            grouped.setdefault(err_text, []).append(where)
        for err_text, where_list in grouped.items():
            where_detail = ", ".join(where_list)
            status.add_error(
                f"{_modbus_client_label(client)}: read failure on {len(where_list)} block(s) "
                f"[{where_detail}]: {err_text}"
            )

    return [process_status.SUCCESS, data_points]


def _modbus_attr_value_for_stream(entry) -> object:
    """
    Coils and discrete inputs are always returned as int 0/1 (or list of 0/1).
    Holding and input registers keep the decoded value type (int/float/list).
    """
    v = entry.value
    kind = getattr(entry, "kind", None)
    if kind is None:
        # Rows from read_data may omit .kind; infer coil vs register from canonical tag prefix.
        kind, _start, _span = _parse_tag((entry.tag or "").strip())
    if kind in ("coil", "discrete"):
        if v is None:
            return None
        if isinstance(v, (list, tuple)):
            return [1 if bool(x) else 0 for x in v]
        return 1 if bool(v) else 0
    if isinstance(v, tuple):
        return list(v)
    return v


def get_modbus_tag_info(entry, column_name=None):
    """
    Shape one Modbus read into the tuple the PLC streaming path expects (same contract as OPC UA / EtherNet/IP).

    Returns [tag_key, timestamp, attr_name, attr_value]:
    - tag_key: synthetic id ns=0;s=<canonical tag>, e.g. ns=0;s=hr:7, for tag-policy lookups.
    - timestamp: UTC time of the read (Modbus has no per-point timestamp on the wire).
    - attr_name: JSON column name — the physical tag (hr:7) unless column_name is set from map= field keys.
    - attr_value: register/coil value for streaming (coils/discrete as bool, registers as decoded).

    column_name is supplied when map= defined human-readable field names separate from internal hr:/ir: ids.
    """
    tag_key = "ns=0;s=" + entry.tag
    timestamp = get_current_utc_time()
    attr = column_name if column_name else entry.tag
    return [tag_key, timestamp, attr, _modbus_attr_value_for_stream(entry)]
