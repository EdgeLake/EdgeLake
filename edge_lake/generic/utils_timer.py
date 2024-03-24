"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

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
