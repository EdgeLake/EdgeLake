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

import time
from edge_lake.generic.utils_data import seconds_to_hms
from edge_lake.generic.utils_columns import get_current_utc_time, _utc_to_local

# =======================================================================================================================
# Maintain info on scheduled tasks
# =======================================================================================================================
class ScheduledTask:
    def __init__(self, task_id:int, task_name, start_date_time, repeat_time, task_string):
        self.reset(task_id)
        self.name = task_name
        self.start_date_time = start_date_time        # Date and time for first execution of the task
        self.exec_time = time.time()
        self.command = task_string
        self.repeat_time = repeat_time
        self.table_created = False  # this flag will force a create statement
        if task_string.find("TIME(PREVIOUS)") >=0 or task_string.find("TIME(CURRENT)") >= 0:
            self.time_cmd = True  # command with time values that changes every time the command is executed


    def reset(self, task_id:int):
        self.id = task_id
        self.name = None
        self.start_date_time = None
        self.run_time = 0       # last execution time
        self.command = None
        self.time_cmd = False   # command with time values that changes every time the command is executed
        self.exec_time = 0      # The time the command is placed to be executed
        self.repeat_time = 0    # Seconds between executions
        self.table_created = False  # A table that aggregates the data of a repeatable query
        self.mode = "Paused"      # Waiting for execution. Modes are: "Active" or Stopped"
        self.previous_time = None   # Time value forrepeatable queries which use  "TIME(PREVIOUS)" and TIME(CURRENT)" setting
        self.run_counter = 0        # Number of times task was executed
        self.last_ret_value = 0     # ret value of last execution

    # =======================================================================================================================
    # Save ret value of last execution
    # =======================================================================================================================
    def set_ret_val(self, ret_value):
        self.last_ret_value = ret_value
    # =======================================================================================================================
    # Return ret value of last execution
    # =======================================================================================================================
    def get_ret_val(self):
        return self.last_ret_value

    # =======================================================================================================================
    # Count execution
    # =======================================================================================================================
    def count_execution(self):
        self.run_counter += 1

    # =======================================================================================================================
    # get the counter of executions
    # =======================================================================================================================
    def get_execution_count(self):
        return self.run_counter

    # =======================================================================================================================
    # Set the time to which a repeatable query was executed
    # =======================================================================================================================
    def set_previous_time(self, date_time):
        self.previous_time = date_time
    # =======================================================================================================================
    # Return the time to which a repeatable query was executed
    # =======================================================================================================================
    def get_previous_time(self):
        return self.previous_time

    # =======================================================================================================================
    # Set the task mode - "active" / paused, stopped
    # =======================================================================================================================
    def set_mode(self, new_mode):
        self.mode = new_mode
    # =======================================================================================================================
    # Return the task mode - "active" / paused, stopped
    # =======================================================================================================================
    def get_mode(self):
        return self.mode

    # =======================================================================================================================
    # test that the start time assigned to the task is valid
    # =======================================================================================================================
    def is_start_time(self):

        if self.mode == "Active":
            return True     # No need to test date as task is active
        current_utc = get_current_utc_time("%Y-%m-%d %H:%M:%S.%f")
        return current_utc >= self.start_date_time

    # =======================================================================================================================
    # RSet start time
    # =======================================================================================================================
    def set_start_time(self, start_date_time):
        self.start_date_time = start_date_time

    # =======================================================================================================================
    # Return the starting date and time for executing the task
    # =======================================================================================================================
    def get_start_time(self):
        return _utc_to_local(self.start_date_time, "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f")
    # =======================================================================================================================
    # Return the Name associated with the task
    # =======================================================================================================================
    def get_name(self):
        return self.name
    # =======================================================================================================================
    # Get the repeat time
    # =======================================================================================================================
    def get_repeat_time(self):
        return self.repeat_time
    # =======================================================================================================================
    # Get formatted repeat time
    # =======================================================================================================================
    def get_formatted_repeat_time(self):
        hours_time, minutes_time, seconds_time = seconds_to_hms(self.repeat_time)
        time_str = "%u:%u:%u" % (hours_time, minutes_time, seconds_time)
        return time_str

   # =======================================================================================================================
    # Test if the process needs to be executed
    # =======================================================================================================================
    def is_exec_time(self):

        if not self.run_time:
            ret_val = True  # never executed    - return True as it needs to be executed
        else:
            current_time = int(time.time())  # current time im seconds
            if current_time >= (self.run_time + self.repeat_time):
                ret_val = True
            else:
                ret_val = False
        return ret_val
    # =======================================================================================================================
    # set the run time as curent time - this method is called when a job is executed.
    # =======================================================================================================================
    def set_run_time(self):
        self.run_time = int(time.time())  # current time im seconds
    # =======================================================================================================================
    # Get Tasl command
    # =======================================================================================================================
    def get_task_command(self):
        return self.command

    # =======================================================================================================================
    #  Is this command with time values that changes every time the command is executed
    # =======================================================================================================================
    def is_time_cmd(self):
        return self.time_cmd

    # =======================================================================================================================
    # When a table for a repeatable query is used:
    # flag that the table was created - such that in the next run of the query the table is not deleted
    # =======================================================================================================================
    def set_table_created(self):
        self.table_created = True  # this flag will avoid delete of the local data in  the table
    # =======================================================================================================================
    # When a table for a repeatable query is used:
    # False is returned on the first call, than the aggregation table is created and is not dropped between queries
    # =======================================================================================================================
    def is_table_created(self):  # only once returns False
        return self.table_created