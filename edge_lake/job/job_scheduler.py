"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import threading
import time

import edge_lake.job.job_instance as job_instance
import edge_lake.job.job_handle as job_handle


JOB_INSTANCES = 100  # non-scheduled (run once) jobs

unique_job_id = 0
job_location = JOB_INSTANCES

counter_mutex = threading.Lock()  # variable on the class level are static

active_jobs = []  # an array holding processed queries

# =======================================================================================================================
# Initiate the dynamic job instances - these are used for non-scheduled (run once) jobs
# =======================================================================================================================
def initiate():
    for x in range(JOB_INSTANCES):
        active_jobs.append(job_instance.JobInstance(x))  # set an array of processed queries

# =======================================================================================================================
# Get a job ny ID
# =======================================================================================================================
def get_job(job_id):
    return active_jobs[job_id]


# =======================================================================================================================
# get the most recent active job
# =======================================================================================================================
def get_recent_job():
    last_job = 0
    for x in range(JOB_INSTANCES):
        if active_jobs[x].get_start_time():  # this is a job which was processed
            if active_jobs[x].get_start_time() > active_jobs[last_job].get_start_time():
                last_job = x
    return last_job


# =======================================================================================================================
# get unique counter to identify the job and location in active_jobs to manage the process
# If all locations are used by running jobs, take the oldest
# =======================================================================================================================
def start_new_job():
    global unique_job_id
    global job_location
    global counter_mutex

    loops_count = 0
    counter = 0
    ret_val = True

    counter_mutex.acquire()

    unique_job_id += 1
    if unique_job_id == 100000000:  # restart counter
        unique_job_id = 1

    while 1:
        job_location += 1
        if job_location >= JOB_INSTANCES:
            job_location = 0

        if not active_jobs[job_location].is_job_active():
            break

        counter += 1
        if counter >= JOB_INSTANCES:
            # sleep to let jobs finish
            time.sleep(2)
            counter = 0
            loops_count += 1
            if loops_count > 5:
                ret_val = False
                break

    if ret_val:
        location = job_location
        active_jobs[location].set_active(counter)
    else:
        location = -1  # Failed to find JOB instance to use

    counter_mutex.release()

    return [location, unique_job_id]

# =======================================================================================================================
# copy the values of one job_handle to another (src tp dest). Reference to the same object does not work in this case
# =======================================================================================================================
def copy_job_handle_info(dest_handle: job_handle, src_handle: job_handle):
    dest_handle.set_output_socket(src_handle.get_output_socket())
    dest_handle.set_output_into(src_handle.get_output_into())           # If output generates HTML file
    if src_handle.is_rest_caller():
        dest_handle.set_rest_caller()
    dest_handle.copy_cmd_conditions(src_handle.get_conditions())  # needs to be a deep copy
    dest_handle.set_subset(src_handle.is_subset())       # If subset flag is True, provide partial results (even if not all nodes replied)
    dest_handle.set_timeout(src_handle.get_timeout())  # timeout determines the max execution time
    dest_handle.set_assignment(src_handle.get_assignment())  # Assign returned value to a key in the dictionary
# =======================================================================================================================
# Test valid job
# =======================================================================================================================
def is_valid_job_id(job_id: str):

    if not job_id.isdecimal():
        return False

    if int(job_id) >= JOB_INSTANCES:
        return False  # allow "all" or a number
    return True