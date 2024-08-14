"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import threading
import time
import traceback

import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.process_log as process_log
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.tcpip.message_header as message_header


system_threads_pools_ = {
    # The lists of continues system threads using a pool
}

system_threads_names_ = {
    # Convert lower to the stored name
    # rest  --> REST, query -- > Query
}
def set_system_pool(pool_name, pool_obj):
    system_threads_names_[pool_name.lower()] = pool_name
    system_threads_pools_[pool_name] = pool_obj

# =======================================================================================================================
# List of system threads and their status
# get system threads
# get system threads where pool = tcp and with_details = true and reset = true
# =======================================================================================================================
def get_system_threads(status, io_buff_in, cmd_words, trace):

    info = ""
    #                             Must     Add      Is
    #                             exists   Counter  Unique
    keywords = {"pool": ("str", False, False, True),        # The name of the pool
                "details": ("bool", False, False, True),    # Include details
                "reset": ("bool", False, False, True),      # Reset statistics
                }
    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)

    if not ret_val:
        pool = interpreter.get_one_value_or_default(conditions, "pool", None)
        details = interpreter.get_one_value_or_default(conditions, "details", False)
        reset = interpreter.get_one_value_or_default(conditions, "reset", False)

        if pool:
            if pool in system_threads_names_:
                # Get the saved name
                pool_name = system_threads_names_[pool]
            else:
                pool_name = None
            if pool_name and pool_name in system_threads_pools_:
                # A single pool
                if  system_threads_pools_[pool_name]:
                    # If not Null
                    info += ("\r\n" + system_threads_pools_[pool_name].get_info(details, reset))
            else:
                status.add_error(f"Command 'get system threads' - Error in pool name: '{pool}'")
                ret_val = process_status.ERR_command_struct
        else:
            # all pools
            for key, pool in system_threads_pools_.items():
                if pool:
                    info += ("\r\n" + pool.get_info(details, reset))

    return [ret_val, info]

# --------------------------------------------------------------------------------
# Read Write Lock
# --------------------------------------------------------------------------------
class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers = 0       # SHould be no more than 1

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notify_all()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()
        self._writers += 1

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release()
        self._writers -= 1

# --------------------------------------------------------------------------------
# Threads Pool
# --------------------------------------------------------------------------------
class WorkersPool:
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, pool_name, threads_count, anylog_cmd = True):

        self.active_threads = True
        if not threads_count:
            threads_count = 1  # Minimum of 1 thread
        self.pool_name = pool_name
        self.threads_count = threads_count
        self.workers_count = threads_count
        self.workers_status = [-1] * threads_count
        self.workers_counter = [0] * threads_count
        self.al_cmd = anylog_cmd        # If True - execute AnyLog command, else Thread executing the cmd

        self.tasks_que = TasksQue(threads_count)  # declare a Que for instructions to the threads

        thread_name = pool_name.lower().replace(' ','_')

        set_system_pool(self.pool_name, self)

        # init threads
        for i in range(threads_count):
            t = threading.Thread(target=self.__exec_task, args=("dummy", i), name=f"{thread_name}_{i}")
            t.start()

    # --------------------------------------------------------------------------------
    # Threads waiting for tasks on the Queries
    # STATISTICS comment - because threads are moving in the position on the list -
    # Statistcs assigns the counter to the thread that would be used assuming static position
    # --------------------------------------------------------------------------------
    def __exec_task(self, dummy, thread_id):

        while 1:
            # worker loop forever - or until exit message

            self.workers_status[thread_id] = 0  # thread goes to sleep

            current_task = self.tasks_que.get()  # wait for a task

            self.workers_status[thread_id] = 1  # Thread executes task

            on_list = self.tasks_que.get_threads_on_list()  # The number of threads on the free list
            if not on_list:
                self.workers_counter[thread_id] += 1  # Count the number of times it executed - all threads are out
            else:
                self.workers_counter[self.threads_count - on_list - 1] += 1  # Count the number of times it executed - see STATISTICS comment above

            # Execute task
            if self.al_cmd:
                # Execute an AnyLog Command
                if self.run(current_task) == process_status.EXIT:
                    break
            else:
                # Execute provided command with the pool thread
                if self.method(current_task) == process_status.EXIT:
                    break

            self.tasks_que.free_list_put(current_task)  # Allow a different thread to reuse the task object

        self.workers_status[thread_id] = -1

        process_log.add_and_print("event", "\nWorker %u/%u of '%s' terminating" % (
        thread_id + 1, self.threads_count, self.pool_name))


    # --------------------------------------------------------------------------------
    # Execute method with a thread from the pool
    # --------------------------------------------------------------------------------
    def method(self, task):

        commnad = task.get_cmd()
        if not commnad:
            ret_val = process_status.EXIT
        else:
            ret_val = process_status.SUCCESS
            args = task.get_args()

            try:
                commnad(*args)
            except:
                utils_print.output("AnyLog thread failed to execute python process", True)

        return ret_val

    # --------------------------------------------------------------------------------
    # Execute an AnyLog process with this thread
    # --------------------------------------------------------------------------------
    def run(self, task):

        mem_view = task.get_mem_view()
        status = task.get_status()

        status.reset()

        commnad = task.get_cmd()
        if not commnad:
            ret_val = process_status.EXIT
        else:
            args = task.get_args()

            if member_cmd.is_debug_method("exception"):
                # allow stacktrace with failure
                # COnfigure using command line: debug on exception
                ret_val = commnad(status, mem_view, *args)
            else:
                try:
                    ret_val = commnad(status, mem_view, *args)
                except Exception as e:
                    stack_data = traceback.format_exc()
                    err_msg = "AnyLog thread failed to execute command: %s" % str(e)
                    status.add_error(err_msg)
                    utils_print.output(err_msg, True)
                    utils_print.output(stack_data, True)

                    ret_val = process_status.ERR_process_failure
                finally:
                    pass

        return ret_val

    # --------------------------------------------------------------------------------
    # Test if threads terminated - go over the threads stats to see if any is available
    # --------------------------------------------------------------------------------
    def all_threads_terminated(self):
        for x in range(self.workers_count):
            if self.workers_status[x] != -1:  # -1 is thread terminated
                return False
        return True  # all at -1 status

    # --------------------------------------------------------------------------------
    # Get an empty task from the Que
    # --------------------------------------------------------------------------------
    def get_free_task(self):
        return self.tasks_que.free_list_get()  # get from the free list or wait on the free list

    # --------------------------------------------------------------------------------
    # Add a new task to be processed
    # --------------------------------------------------------------------------------
    def add_new_task(self, task):
        self.tasks_que.put(task)  # Place a task on the free list

    # --------------------------------------------------------------------------------
    # Add a new task to be processed
    # --------------------------------------------------------------------------------
    def ignore_task(self, task):
        self.tasks_que.free_list_put(task) # Place a task on the free list

    # --------------------------------------------------------------------------------
    # Return Info on the Pool
    # --------------------------------------------------------------------------------
    def get_info(self, with_details, with_reset):

        if with_reset:
            # reset the counter
            for thread_id in range (self.threads_count):
                self.workers_counter[thread_id] = 0

        if with_details:
            info_list = []
            title_list = ["Thread", "Status", "Calls", "Percentage"]
            total_calls = 0
            for thread_id in range (self.threads_count):
                thread_status = self.workers_status[thread_id]
                thread_counter = self.workers_counter[thread_id]    # Number of calls
                total_calls += thread_counter
                info_list.append( [ thread_id, thread_status, thread_counter, 0] )
            # Set the percentages of usage of each thread
            if total_calls:
                total_percentage = 0
                for thread_id in range (self.threads_count):
                    thread_counter = info_list[thread_id][2]
                    percentage = int(thread_counter * 100 /  total_calls)
                    if thread_id < (self.threads_count - 1):
                        # not the last
                        info_list[thread_id][3] = percentage
                        total_percentage += percentage
                    else:
                        info_list[thread_id][3] = 100 - total_percentage        # To ignore the roundings

            header = f"{self.pool_name} Pool:"
            info_str = utils_print.output_nested_lists(info_list, header, title_list, True)

        else:
            info_str = f"{self.pool_name} Pool with {self.workers_count} threads:".ljust(60) + str(self.workers_status)

        return info_str

    # --------------------------------------------------------------------------------
    # Return the number of threads
    # --------------------------------------------------------------------------------
    def get_number_of_threds(self):
        return self.workers_count

    # --------------------------------------------------------------------------------
    # End Pool process by providing a task with NULL command to each thread
    # --------------------------------------------------------------------------------
    def exit(self):

        set_system_pool(self.pool_name, None)

        self.active_threads = False
        for x in range(self.threads_count):
            task = self.get_free_task()
            task.set_cmd(None, None)  # None will make the thread to terminate
            self.add_new_task(task)



    # --------------------------------------------------------------------------------
    # Returns FALSE if pool was terminated
    # --------------------------------------------------------------------------------
    def is_active(self):
        return self.active_threads


# --------------------------------------------------------------------------------
# Tasks Que
# take new tasks from the root and end to the leaf
# --------------------------------------------------------------------------------
class TasksQue:
    def __init__(self, threads_count):

        self.root = None
        self.leaf = None
        self.free_list = None
        self.threads_on_list = threads_count     # Number of threads on the free list

        self.event_new_task = threading.Condition()
        self.event_free_task = threading.Condition()

        for i in range(threads_count):
            new_task = Task(i)
            new_task.set_next(self.free_list)
            self.free_list = new_task

    # --------------------------------------------------------------------------------
    # Get new task to execute - tasks are taken from the leaf
    # --------------------------------------------------------------------------------
    def get(self):

        self.event_new_task.acquire()

        while 1:
            # loop until task is available
            task = self.leaf
            if task:
                break
            self.event_new_task.wait()  # wait until signaled

        self.leaf = task.get_previous()  # previous task is the next one to execute
        if not self.leaf:
            self.root = None

        self.event_new_task.release()

        return task

    # --------------------------------------------------------------------------------
    # Add new task to execute - new tasks are added at the root
    # --------------------------------------------------------------------------------
    def put(self, new_task):

        new_task.set_previous(None)

        self.event_new_task.acquire()

        if self.root:
            # root becomes second in list - connect to new root
            self.root.set_previous(new_task)  # new task is set as the new root
        else:
            self.leaf = new_task  # First task on the list

        new_task.set_next(self.root)

        self.root = new_task  # previous task is the next one to execute

        self.event_new_task.notify()  # wake one thread

        self.event_new_task.release()

    # --------------------------------------------------------------------------------
    # Get task from the free list
    # --------------------------------------------------------------------------------
    def free_list_get(self, with_tasks = False):
        '''
        with_tasks -  The thread asking for a task is holding tasks and can wait forever.
                     In this case - A NULL is return and the asking thread is expected to release tasks before the call to free_list_get
        '''

        self.event_free_task.acquire()

        while 1:
            # loop until task is available
            free_task = self.free_list
            if free_task:
                break
            if with_tasks:
                self.event_free_task.release()
                return None     # The calling thread needs to release tasks first

            self.event_free_task.wait()  # wait until signaled

        self.free_list = free_task.get_next()

        self.threads_on_list -= 1  # Count threads on free list - need to be atomic

        self.event_free_task.release()

        return free_task

    # --------------------------------------------------------------------------------
    # Place task on the free list
    # --------------------------------------------------------------------------------
    def free_list_put(self, free_task):

        self.event_free_task.acquire()

        free_task.set_next(self.free_list)

        self.free_list = free_task

        self.threads_on_list += 1  # Count threads on free list - need to be atomic

        self.event_free_task.notify()  # wake one thread

        self.event_free_task.release()

    # --------------------------------------------------------------------------------
    # Return the number of threads on the list
    # --------------------------------------------------------------------------------
    def get_threads_on_list(self):
        return self.threads_on_list
# --------------------------------------------------------------------------------
# Task Object
# --------------------------------------------------------------------------------
class Task:

    def __init__(self, task_id):
        self.task_id = task_id
        self.next = None  # used in the task list and the free list
        self.previous = None  # used in the task list
        self.command = None  # Command to execute
        self.args = None  # command arguments

        self.status = process_status.ProcessStat()

        buff_size = int(params.get_param("io_buff_size"))
        self.mem_view = memoryview(bytearray(buff_size))

    def get_task_id(self):
        return self.task_id

    def get_next(self):
        return self.next

    def get_previous(self):
        return self.previous

    def set_next(self, next_task):
        self.next = next_task

    def set_previous(self, previous_task):
        self.previous = previous_task

    def set_cmd(self, cmd, args):
        self.command = cmd
        self.args = args

    def get_cmd(self):
        return self.command

    def get_args(self):
        return self.args

    def get_status(self):
        return self.status

    def get_mem_view(self):
        return self.mem_view

    # --------------------------------------------------------------------------------
    # Copy the task provider Io buffer to the local IO buffer
    # --------------------------------------------------------------------------------
    def set_io_buff(self, io_buff_in):
        message_header.copy_block(self.mem_view, io_buff_in)

# --------------------------------------------------------------------------------
# Get the thread number from the thread name
# --------------------------------------------------------------------------------
def get_thread_name():
    try:
        thread_name =  threading.current_thread().name.lower()
    except:
        thread_name = "not_available"
    else:
        thread_name = thread_name.replace(" ", "_")

    return thread_name

# --------------------------------------------------------------------------------
# Print thread message
# --------------------------------------------------------------------------------
def print_thread_message(message):
    t_name = get_thread_name()
    utils_print.output(f"\r\nThread: {t_name} Message: {message}", False)

# --------------------------------------------------------------------------------
# Get the thread number from the thread name
# --------------------------------------------------------------------------------
def get_thread_number():
    name = threading.current_thread().name
    # Name is "Thread-x" whereas x is a number
    if len(name) > 7 and name[7:].isnumeric():
        thread_number = int(name[7:])
    else:
        thread_number = 0
    return thread_number

# --------------------------------------------------------------------------------
# Place a thread on sleep
# --------------------------------------------------------------------------------
def seconds_sleep(process_name, seconds):

    try:
        time.sleep(seconds)
    except:
        utils_print.output("Keyboard Interrupt - exit from sleep(%u) in process: %s" % (seconds, process_name), True)
        ret_val = False
    else:
        ret_val = True

    return ret_val

