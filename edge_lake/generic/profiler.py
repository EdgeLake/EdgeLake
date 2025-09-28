
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import io

# Profile Flag
import os
profiler_active_ = True if os.getenv("PROFILER", "False").lower() == "true" else False      # Needs to return True - otherwise will be False
if profiler_active_:
    try:
        import cProfile
        import pstats       # class is used to format the profiling results.
    except:
        profiler_active_ = False      # Failed
    else:
        profiler_active_ = True

    import edge_lake.generic.utils_print as utils_print



profile_unit_ = {}

profile_targets_ = {            # The list of profiled services
    "operator" : {
        "set_on" : False,       # Instruction from main thread
        "is_on" : False,
        "reset" : False,
        "profiler" : None
    },
    "get": {  # REST GET
        "set_on": False,  # Instruction from main thread
        "is_on": False,  # Was it set by the profiled thread
        "reset": False,
        "profiler": None
    },
    "put": {                    # REST PUT
        "set_on" : False,       # Instruction from main thread
        "is_on": False,         # Was it set by the profiled thread
        "reset": False,
        "profiler" : None
    },
    "post": {                    # REST POST
        "set_on" : False,       # Instruction from main thread
        "is_on": False,         # Was it set by the profiled thread
        "reset": False,
        "profiler" : None
    },
}

# -----------------------------------------------------------------
# Was stopped
# _________________________________________________________________
def is_running(target):
    return profile_targets_[target]["is_on"]      # return True if profiling is active

# -----------------------------------------------------------------
# Validate target
# _________________________________________________________________
def is_target(target):
    return target in profile_targets_
# -----------------------------------------------------------------
# Return True if lib loaded
# _________________________________________________________________
def is_active():
    return profiler_active_
# ----------------------------------------------------------------------------------------------
# Get a new profiler instance
# ----------------------------------------------------------------------------------------------
def profiler_new_instance():

    if profiler_active_:
        try:
            profiler = cProfile.Profile()
        except:
            utils_print.output_box("Failed to use cProfile", "blue")
            profiler = None

    else:
        utils_print.output_box("cProfile not active", "blue")
        profiler = None

    return profiler

# ----------------------------------------------------------------------------------------------
# Start Profiler instance
# ----------------------------------------------------------------------------------------------
def profiler_start(profiler):

    if profiler_active_:
        try:
            profiler.enable()
        except:
            utils_print.output_box("Failed to start cProfile","blue")
            ret_val = False
        else:
            ret_val = True
    else:
        utils_print.output_box("cProfile not active","blue")
        ret_val = False

    return ret_val

# ----------------------------------------------------------------------------------------------
'''
ncalls: This column shows the number of times a function was called. It counts both direct calls to the function and recursive calls if applicable.

tottime: Total time spent in the function, excluding time spent in calls to sub-functions. It represents the accumulated time spent executing the code inside the function directly.

percall (for tottime): Average time spent per call to the function. It's calculated as tottime / ncalls.

cumtime: Cumulative time spent in the function and all functions that it calls (directly or indirectly). It includes the time spent in sub-functions called by the function. This is also known as inclusive time.

percall (for cumtime): Average cumulative time per call, which is cumtime / ncalls.

'''
# ----------------------------------------------------------------------------------------------
def get_profiler_results(target):


    if profiler_active_:

        profiler = profile_targets_[target]["profiler"]
        if profiler:
            try:
                s = io.StringIO()

                ps = pstats.Stats(profiler, stream=s)

                # Filter stats to include only functions with cumulative time > 0.1 seconds
                ps.strip_dirs().sort_stats(pstats.SortKey.CUMULATIVE).print_stats(100)

                # Get the profiling results as a string
                profiling_results = s.getvalue()

            except:
                profiling_results = "Failed to print cProfile"
        else:
            profiling_results = "cProfile not active"
    else:
        profiling_results = "Profiler for %s not available" % target


    return profiling_results


# ----------------------------------------------------------------------------------------------
# End Profiler
# ----------------------------------------------------------------------------------------------
def profiler_end(profiler):

    if profiler_active_:
        try:
            profiler.disable()
        except:
            utils_print.output_box("Failed to disable cProfile","blue")
            ret_val = False
        else:
            ret_val = True

    else:
        utils_print.output_box("cProfile not active","blue")
        ret_val = False
    return ret_val


# ----------------------------------------------------------------------------------------------
# Set profile instruction
# The main thread determines on, off, or reset for the profile status
# ----------------------------------------------------------------------------------------------
def set_instructions(target, is_on, reset_stat):
    '''
    target - operator, REST, BROKER, TCP
    is_on - true/false
    reset_stat - -1 unchanged, 0 - False, 1 - True
    '''

    target_info = profile_targets_[target]

    if target_info["set_on"] == is_on:
        return False

    target_info["set_on"] = is_on

    if is_on:
        # Modified when profilers starts (not when ends)
        target_info["reset"] = reset_stat

    return True

# ----------------------------------------------------------------------------------------------
# The process by the profiled thread
# It starts / stops profiling
# ----------------------------------------------------------------------------------------------
def manage(target):

    target_info = profile_targets_[target]
    if target_info["set_on"] and not target_info["is_on"]: # Instruction to start and was not set
        # Start
        if not target_info["profiler"] or target_info["reset"]:
            # get profiler
            profiler = profiler_new_instance()
            if profiler:
                target_info["profiler"] = profiler
                target_info["reset"] = False    # New profiler is reset
        else:
            profiler = target_info["profiler"]

        if profiler:
            if profiler_start(profiler):
                # started
                target_info["is_on"] = True

    elif not target_info["set_on"] and target_info["is_on"]: # Instruction to stop and is still running

        profiler = target_info["profiler"]
        if profiler:
            if profiler_end(profiler):
                target_info["is_on"] = False

# ----------------------------------------------------------------------------------------------
# A process by the profiled thread
# Stop profiling
# ----------------------------------------------------------------------------------------------
def stop(target):

    target_info = profile_targets_[target]
    if target_info["is_on"]: # If is ON - then stop
        profiler = target_info["profiler"]
        if profiler:
            profiler_end(profiler)
            target_info["is_on"] = False









