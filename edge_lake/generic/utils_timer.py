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
# =======================================================================================================================
# Creates timmers for different processes
# =======================================================================================================================
import time


class ProcessTimer:

    def __init__(self, num_of_counters):
        self.timers_count = num_of_counters
        self.total_time = [0 for _ in range(num_of_counters)]
        self.start_time = [0 for _ in range(num_of_counters)]
        self.stopped = [False for _ in range(num_of_counters)]

    # ===============================================================
    # Start all timers
    # ===============================================================
    def start_all(self):
        current_time = time.time()
        for x in range(self.timers_count):
            self.start_time[x] = current_time
            self.total_time[x] = 0
            self.stopped[x] = False

    # ===============================================================
    # Pause a timer by adding its time to the total
    # ===============================================================
    def pause(self, counter_id):
        delta_time = time.time() - self.start_time[counter_id]
        self.total_time[counter_id] += delta_time

    # ===============================================================
    # start a timer by reseting the start time
    # ===============================================================
    def start(self, counter_id):
        self.start_time[counter_id] = time.time()

    # ===============================================================
    # Get timer total time
    # ===============================================================
    def get_timer(self, counter_id):
        elapsed_time = int(self.total_time[counter_id])
        if not elapsed_time and self.start_time[counter_id]:
            # Start was called - but paused was not called:
            elapsed_time = int(time.time() - self.start_time[counter_id])
        return elapsed_time

    # ===============================================================
    # Stop a timer - if the timmer is running - adding its time to the total
    # ===============================================================
    def stop(self, counter_id):
        if not self.stopped[counter_id]:
            delta_time = time.time() - self.start_time[counter_id]
            self.total_time[counter_id] += delta_time
            self.stopped[counter_id] = True
