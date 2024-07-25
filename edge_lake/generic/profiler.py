
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import io

try:
    import cProfile
    import pstats       # class is used to format the profiling results.
except:
    profiler_active = False      # Failed
else:
    profiler_active = True

import edge_lake.generic.utils_print as utils_print


# ----------------------------------------------------------------------------------------------
# Start Profiler instance
# ----------------------------------------------------------------------------------------------
def profiler_start():

    if profiler_active:
        try:
            profiler = cProfile.Profile()
            profiler.enable()
        except:
            utils_print.output_box("Failed to use cProfile")

    else:
        utils_print.output_box("cProfile not active")
        profiler = None

    return profiler

# ----------------------------------------------------------------------------------------------
'''
ncalls: This column shows the number of times a function was called. It counts both direct calls to the function and recursive calls if applicable.

tottime: Total time spent in the function, excluding time spent in calls to sub-functions. It represents the accumulated time spent executing the code inside the function directly.

percall (for tottime): Average time spent per call to the function. It's calculated as tottime / ncalls.

cumtime: Cumulative time spent in the function and all functions that it calls (directly or indirectly). It includes the time spent in sub-functions called by the function. This is also known as inclusive time.

percall (for cumtime): Average cumulative time per call, which is cumtime / ncalls.

'''
# ----------------------------------------------------------------------------------------------
def get_profiler_results(profiled_name, profiler):

    profiling_results = None

    utils_print.output_box("Profiled Moudle: %s" % profiled_name)

    if profiler_active:
        try:
            s = io.StringIO()

            ps = pstats.Stats(profiler, stream=s)

            # Filter stats to include only functions with cumulative time > 0.1 seconds
            ps.strip_dirs().sort_stats(pstats.SortKey.CUMULATIVE).print_stats(0.1)

            # Get the profiling results as a string
            profiling_results = s.getvalue()

        except:
            utils_print.output_box("Failed to print cProfile")
    else:
        utils_print.output_box("cProfile not active")

    return profiling_results


# ----------------------------------------------------------------------------------------------
# End Profiler
# ----------------------------------------------------------------------------------------------
def profiler_end(profiler):

    if profiler_active:
        try:
            profiler.disable()
        except:
            utils_print.output_box("Failed to disable cProfile")

    else:
        utils_print.output_box("cProfile not active")






