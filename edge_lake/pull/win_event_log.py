"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# PULL From Windows EventLog

import sys

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print

try:
    import win32evtlog
except:
    winlog_ = False
else:
    winlog_ = True

win_event_map_ = {
    "error" : 1,
    "warning" : 2,
    "information" : 4,
    "audit_success" : 8,
    "audit_failure" : 16,
    "all" : 0x1f
}
# ------------------------------------------------------------------------------
# Prepare the windows Event Log Process
# This method updates returned values in dispatch_params: hand, flags, last_record
# ------------------------------------------------------------------------------
def prep_win_event_log(status, prep_params, dispatch_params):

    conditions = prep_params['conditions']
    source = prep_params['source']
    process_info = prep_params['process_info']

    hand = None
    flags = None
    last_record = None

    if winlog_ == False:
        err_msg = "Windows library win32evtlog is not loaded"
        status.add_error(err_msg)
        ret_val = process_status.Failed_to_import_lib
    else:

        try:

            ret_val = process_status.SUCCESS
            err_msg = ""

            flags = win32evtlog.EVENTLOG_FORWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ
            # last_record - Index (offset) in the event log
            # Tells ReadEventLog() where to start reading (e.g., from record 0, or from the last saved offset)

            log_type = 'System' if not "log_type" in conditions else conditions["log_type"][0]      # Default
            process_info["log_type"] = log_type

            # hand - like a file descriptor -
            # Tells ReadEventLog() which log to read from (e.g., System, Application)
            hand = win32evtlog.OpenEventLog(source, log_type)
            total = win32evtlog.GetNumberOfEventLogRecords(hand)

            num, oldest = win32evtlog.GetNumberOfEventLogRecords(hand), win32evtlog.GetOldestEventLogRecord(hand)
            last_record = oldest + num - 1

            if "event_type" in conditions:
                events_list = conditions["event_type"]
                for event in events_list:
                    if event in win_event_map_:
                        process_info["win_flag"] |= win_event_map_[event]
                        if not process_info["event_type"]:
                            process_info["event_type"] = event
                        else:
                            process_info["event_type"] += f",{event}"
                    else:
                        ret_val = process_status.ERR_command_struct
                        err_msg = "Unknown event type '%s' for Windows Event Log" % event
                        status.add_error(err_msg)
                        break
            else:
                process_info["win_flag"] = win_event_map_["error"]  # All events
                process_info["event_type"] = "error"
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = f"Process 'run scheduled pull' failed: {value}"
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure

    # The values below are picked by the pull process
    dispatch_params["hand"] = hand
    dispatch_params["flags"] = flags
    dispatch_params["last_record"] = last_record

    return [ret_val, err_msg]

# ----------------------------------------------------------------------------------------
# Read from windows eventlogs
# ----------------------------------------------------------------------------------------
def get_windows_events(status, dispatch_params):
    '''
    status - object status
    process_info - info on the status of the process and the execution
    hand - like a file descriptor - Tells ReadEventLog() which log to read from (e.g., System, Application)
    flags -read flags
    last_record - Index (offset) in the event log - Tells ReadEventLog() where to start reading (e.g., from record 0, or from the last saved offset)
    '''

    process_info = dispatch_params.get("process_info", None)
    hand = dispatch_params.get("hand", None)
    flags = dispatch_params.get("flags", None)
    last_record = dispatch_params.get("last_record", None)

    win_events = process_info["win_flag"]   # Determine the type of log events to consider

    try:
        events = win32evtlog.ReadEventLog(hand, flags, last_record)
        new_last_record = events[-1].RecordNumber if len(events) else last_record
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f"WWindows read from Event Log Failed: {value}"
        status.add_error(err_msg)
        utils_print.output_box(err_msg)
        ret_val = process_status.ERR_process_failure
        new_last_record = last_record
        records = None
    else:
        ret_val = process_status.SUCCESS

        records = []
        try:
            for event in events:
                if event.EventType & win_events:  # bitmap to determine which events to pull
                    records.append({
                        "timestamp": event.TimeGenerated.strftime("%Y-%m-%d %H:%M:%S"),
                        "record_number": event.RecordNumber,
                        "event_id": event.EventID & 0xFFFF,
                        "source_name": event.SourceName,
                        "message": event.StringInserts
                    })
        except:
            ret_val = process_status.ERR_process_failure
            status.add_error("Failed to parse Windows Event Log")

    dispatch_params["last_record"] = new_last_record

    return [ret_val, records]
