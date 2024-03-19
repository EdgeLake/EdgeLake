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
from datetime import datetime, timedelta

import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
import edge_lake.generic.process_log as process_log
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.job.sche_task as sche_task
from edge_lake.generic.utils_print import output_nested_lists
from edge_lake.generic.utils_columns import get_current_utc_time

scheduled_tasks = []        # a 2 dimensional array holding scheduled tasks as a [scheduler id][task whaeas entry 0 is not used]

scheduler_time = []         # not active, when active, time is set to 0 or the number of seconds

counter_schedulers = 0     # The number of schedulers running


# --------------------------------------------------------------
# Return True if running
# Test the user scheduler at location 1 (not the system scheduler which is at location 0)
# This method satisfies the command: get processes --> indicating users declared processes
# --------------------------------------------------------------
def is_running():
    global scheduler_time

    return not all(x == -1 for x in scheduler_time)
# --------------------------------------------------------------
# Terminate a scheduler by by setting the schedular time to -1
# --------------------------------------------------------------
def set_not_active(sched_id):
    global scheduler_time

    if sched_id < len (scheduler_time):
        scheduler_time[sched_id] = -1       # The scheduler thread will exit

# --------------------------------------------------------------
# Return the list of active schedulers running
# --------------------------------------------------------------
def get_info(status):

    if is_running():
        info = "Schedulers IDs in use:"
        for id, value in enumerate(scheduler_time):
            if value != -1:
                if id == 0:
                    info += " [0 (system)]"
                elif id == 1:
                    info += " [1 (user)]"
                else:
                    info += " [%u]" % id
    else:
        info = ""
    return info

# =======================================================================================================================
# Start a new scheduler
# If ID is not provided, starting Scheduler #1 (if not operational)
# =======================================================================================================================
def set_scheduler( status, sched_id ):

    global counter_schedulers
    global scheduler_time
    global scheduled_tasks

    if sched_id > 10:
        status.add_error("Scheduler ID is above limit")
        ret_val = process_status.ERR_command_struct
    else:
        if counter_schedulers <= sched_id:
            for i in range (counter_schedulers, sched_id + 1):
                scheduled_tasks.append([None])
                scheduler_time.append(-1)
            counter_schedulers = sched_id + 1

        if scheduler_time[sched_id] == -1:      # This scheduler is not running
            ret_val = process_status.Scheduler_not_active
        else:
            ret_val = process_status.SUCCESS

    return ret_val       # This scheduler is running
# =======================================================================================================================
# Return the Frequency of the scheduler in seconds
# =======================================================================================================================
def get_frequency(sched_id:int):
    global scheduler_time
    if sched_id >= 0 and sched_id < len(scheduler_time):
        frequency = scheduler_time[sched_id]
    else:
        frequency = -1

    return frequency
# =======================================================================================================================
# Return the number of schedulers running
# =======================================================================================================================
def get_schedulers():
    global counter_schedulers
    return counter_schedulers

# =======================================================================================================================
# Run a process to review scheduled tasks every wake_time seconds
# =======================================================================================================================
def schedule_server(dummy: str, sched_id:int, wake_time: int):

    global scheduler_time
    global counter_schedulers
    global scheduled_tasks

    status = process_status.ProcessStat()

    buff_size = int(params.get_param("io_buff_size"))
    data_buffer = bytearray(buff_size)

    process_log.add("event", "Scheduler process every %u seconds is running" % wake_time)

    scheduler_time[sched_id] = wake_time

    while 1:

        if process_status.is_exit("scheduler") or scheduler_time[sched_id] == -1:
            process_log.add_and_print("event", "Scheduler #%u terminated" % sched_id)
            scheduler_time[sched_id] = -1  # flag that scheduler is not running
            break

        scheduled_counter = len(scheduled_tasks[sched_id])     # Number of tasks (+ 1) on the scheduler

        for task_id in range(1, scheduled_counter):

            task = scheduled_tasks[sched_id][task_id]  # Get the scheduled instance

            if task and task.get_mode() != "Stopped" and task.get_mode() != "Removed":  # make sure that all is available
                if task.is_start_time():        # test that the start time assigned to the task is valid
                    if task.is_exec_time():  # test if the job time triggers execution

                        task.set_mode("Active")

                        task.count_execution()      # Counter for the number of executions

                        task.set_run_time()  # update current time as execution time

                        status.reset()

                        status.set_job_id_scheduled(sched_id, task_id)  # save the scheduled id on the status handle

                        command_str = task.get_task_command()

                        command_str = update_command_string(task, command_str, wake_time)

                        ret_val = member_cmd.process_cmd(status, command=command_str, print_cmd=False, source_ip="", source_port="", io_buffer_in=data_buffer)

                        task.set_ret_val(ret_val)

        time.sleep(wake_time)
# =======================================================================================================================
# Add time to command string
# =======================================================================================================================
def update_command_string(task: sche_task, command_str: str, wake_time:int):

    updated_str = command_str

    if task.is_time_cmd():      # Is this command with time values that changes every call

        offset = updated_str.find("TIME(PREVIOUS)")     # Modify the time on the command (like SQL command) between the last run to the current time
        if offset != -1:
            previous_time = task.get_previous_time()
            if not previous_time:
                # For the first time, subtract the scheduler time from the previous time
                previous_time = "'" + str(datetime.now() - timedelta(seconds=wake_time)) + "'"
            updated_str = updated_str[:offset] + previous_time + updated_str[offset + 14:]

        offset = updated_str.find("TIME(CURRENT)")
        if offset != -1:
            time_current = "'" + str(datetime.now()) + "'"
            updated_str = updated_str[:offset] + time_current + updated_str[offset + 13:]
            task.set_previous_time(time_current)

    return updated_str

# =======================================================================================================================
# Get a Task task by ID
# =======================================================================================================================
def get_task(sched_id, task_id):
    '''
    :param sched_id:
    :param task_id:
    :return: sche_task
    '''
    global scheduled_tasks

    if len(scheduled_tasks) <= sched_id:
        return None     # Wrong scheduler ID

    tasks_list = scheduled_tasks[sched_id]
    if not tasks_list or len(tasks_list) <= task_id:
        return None    # No tasks in scheduler

    task = tasks_list[task_id]
    if task and task.get_mode() == "Removed":
        task = None

    return task
# =======================================================================================================================
# Get a Task by name
# =======================================================================================================================
def get_task_by_name(sched_id, task_name):
    '''
    :param sched_id:
    :param task_name:
    :return: sche_task
    '''
    global scheduled_tasks

    if len(scheduled_tasks) <= sched_id:
        return None     # Wrong scheduler ID

    tasks_list = scheduled_tasks[sched_id]
    if not tasks_list or len(tasks_list) <= 1:
        return None    # No tasks in scheduler

    for task in tasks_list:
        if task and task.get_mode() != "Removed" and task.get_name() == task_name:
            return task

    return None        # No tasks with the requested name

# =======================================================================================================================
# Initiate a scheduled task - these are used for scheduled jobs
# =======================================================================================================================
def get_new_task(sched_id, task_name, start_date_time, repeat_time, task_string):

    global scheduled_tasks

    if not start_date_time:
        first_run_time = get_current_utc_time("%Y-%m-%d %H:%M:%S.%f")  # Curret time as the start time for the command
    else:
        first_run_time = start_date_time

    task_set = False

    if len(scheduled_tasks[sched_id]) > 1:
        # Use removed locations
        scheduled_jobs = len(scheduled_tasks[sched_id])
        for task_id in range(1, scheduled_jobs):
            task = get_task(sched_id, task_id)
            if not task:
                task = sche_task.ScheduledTask(task_id, task_name, first_run_time, repeat_time, task_string)  # replace with a new task object
                scheduled_tasks[sched_id][task_id] = task
                task_set = True     # Replaced an existing task that was flagged as "Removed"
                break
    else:
        scheduled_jobs = 1

    if not task_set:
        task = sche_task.ScheduledTask(scheduled_jobs, task_name, first_run_time, repeat_time, task_string)
        scheduled_tasks[sched_id].append(task)  # set an array of processed queries

    return task

# =======================================================================================================================
# Test valid job
# =======================================================================================================================
def is_valid_task_id(sched_id, task_number: str):
    if not task_number.isdecimal():
        return False

    task_id = int(task_number)
    scheduled_jobs = len(scheduled_tasks[sched_id])

    if task_id< 1 or task_id > scheduled_jobs:
        return False  # allow "all" or a number

    return True

# =======================================================================================================================
# Change the mode of a task
# =======================================================================================================================
def change_task_mode(status, sched_id, task_id, operation):

    global scheduled_tasks

    task = scheduled_tasks[sched_id][task_id]
    if operation == "stop":
        task.set_non_active()
    elif operation == "run":
        task.set_active()
    elif operation == "remove":
        scheduled_tasks[sched_id][task_id] = None

    return process_status.SUCCESS

# =======================================================================================================================
# Show all schedulers or one scheduler
# =======================================================================================================================
def show_all( ):

    global counter_schedulers

    info_str = ""
    for sched_no in range (counter_schedulers):
        info_str += "\r\n"
        info_str += show_info(sched_no)

    return info_str
# =======================================================================================================================
# Show scheduled jobs
# command: show scheduler
# =======================================================================================================================
def show_info(sched_id):

    global scheduled_tasks

    if sched_id >= len(scheduler_time):
        info_str = "Scheduler %u not declared" % sched_id
    else:
        scheduled_jobs = len(scheduled_tasks[sched_id])

        if scheduler_time[sched_id] == -1:
            state = "Not Running"
        else:
            state = "Running"
        info_str = "\r\nScheduler ID:     %u\r\nScheduler Status: %s" % (sched_id, state)

        tasks_list = []
        print_columns = ["ID", "Mode", "Name", "Counter", "Run Status", "Start-Time", "Repeat-Time", "Task"]

        for task_id in range(1, scheduled_jobs):
            task = scheduled_tasks[sched_id][task_id]
            counter = task.get_execution_count()        # number of executions
            if counter:
                run_status = process_status.get_status_text( task.get_ret_val() )
            else:
                run_status = "No runs"
            if task and task.get_mode() != "Removed":        # Null is the case that the task was removed
                tasks_list.append((task_id, task.get_mode(), task.get_name(), \
                                   counter, run_status, \
                                   task.get_start_time(), task.get_formatted_repeat_time(), task.get_task_command() ))

        info_str += output_nested_lists(tasks_list, "Scheduled Tasks", print_columns, True, "")

    return info_str

# =======================================================================================================================
# Get the scheduler ID from the command words. If no value - then the default is 1.
# Then determine if the scheduler exists
# =======================================================================================================================
def get_sched_id(status, words_array, word_offset):

    global scheduled_tasks

    if word_offset >= len(words_array):
        sched_id = 1
    else:
        sched_number = words_array[word_offset]
        if not sched_number.isdecimal():
            sched_id = -1               # Not a valid number
            status.add_error("Scheduler ID is not a valid number: '%s'" % sched_number)
        else:
            sched_id = int(sched_number)

    # Test the scheduler exists
    if sched_id >= len(scheduled_tasks):
        status.add_error("Scheduler ID %u is not running" % sched_id)
        sched_id = -1

    return sched_id


