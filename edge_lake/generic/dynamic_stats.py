"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
import time

import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter

statistics_ = {}

# --------------------------------------------------------------------------
# Start measuring time on a key
# --------------------------------------------------------------------------
def start_timer(stat_name:str, key:str, timer_name:str):
    '''
    stat_name : str -   The name of the process
    key : str -   The key of the time statistic
    timer_name : str -   The name of the timer
    '''
    try:
        statistics_[stat_name][key][timer_name][0] = time.time()
    except:
        # Initiate the setup to monitor the time
        try:
            if not stat_name in statistics_:
                statistics_[stat_name] = {}
            if not key in statistics_[stat_name]:
                # Set start time, and total time
                statistics_[stat_name][key] = {}
            if not timer_name in statistics_[stat_name][key]:
                statistics_[stat_name][key][timer_name] = [time.time(), 0]
        except:
            pass

def stop_timer(stat_name:str, key:str, timer_name:str):
    '''
    stat_name : str -   The name of the process
    key : str -   The key of the time statistic
    '''
    try:
        time_diff = time.time() - statistics_[stat_name][key][timer_name][0]
        statistics_[stat_name][key][timer_name][1] += time_diff         # Keep total time
    except:
        pass    # If reset is done while updating the structure


def add_values(stat_name:str, key:str, values:dict):
    '''
    stat_name : str -   The name of the process
    key : str -   The key of the time statistic
    values: dict -     The values to include
    '''

    for one_key, value in values.items():
        try:
            statistics_[stat_name][key][one_key] += value
        except:
            try:
                # Initiate the setup to monitor the time
                if not stat_name in statistics_:
                    statistics_[stat_name] = {}
                if not key in statistics_[stat_name]:
                    # Set start time, and total time
                    statistics_[stat_name][key] = {}
                statistics_[stat_name][key][one_key] = value        # Initialize
            except:
                pass    # If reset is done while updating the structure

# --------------------------------------------------------------------------
# get dynamic stats where name = operator.json
# --------------------------------------------------------------------------
def get_dynamic_stats(status, io_buff_in, cmd_words, trace):
    global statistics_

    #                             Must     Add      Is
    #                             exists   Counter  Unique
    keywords = {"name": ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    stat_name = interpreter.get_one_value(conditions, "name")

    title = ["Table"]
    info_table = []
    if stat_name in statistics_:
        info_dict = statistics_[stat_name]

        totals = ["Total"]                       # Last row are totals
        for key, value in info_dict.items():

            row_info = []              # Collect the info in the output table
            row_info.append(key)      # This would be the dbms.table name
            index = 1
            for one_key, value in value.items():
                if isinstance(value, list):
                    # this is an entry that monitors time which was updated using start_timer
                    use_value = round(value[1], 3)        # Total time
                    column_title = "HH:MM:SS.f"
                else:
                    use_value = value
                    column_title = one_key
                row_info.append(use_value)

                # Update totals (ignore the key)
                if len(totals) <= index:
                    totals.append(use_value)
                else:
                    totals[index] += use_value

                if len(title) <= index:
                    title.append(column_title)
                index += 1

            info_table.append(row_info)


        info_table.append(totals)


    if not len(info_table):
        output_txt = ""
        ret_val = process_status.Empty_result_set
    else:
        first_row = info_table[0]           # First table tow
        for index, column in enumerate(first_row):
            if isinstance(column, float):
                # Format the columns to be 3 decimal points
                for row in info_table:
                    float_val = row[index]
                    seconds = int(float_val)
                    hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(seconds)
                    hhmmss = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)
                    x = float_val- seconds
                    fractions = f"{x:.3f}"
                    row[index] =  hhmmss + fractions[1:]       # Fix all values in the column
            elif isinstance(column,int):
                title[index] += "\t{:,}"                        # Comma seperated

        output_txt = utils_print.output_nested_lists(info_table, "", title, True)
        ret_val = process_status.SUCCESS

    return [ret_val, output_txt]


def reset_dynamic_stats(status, io_buff_in, cmd_words, trace):
    global statistics_
    statistics_ = {}
    return process_status.SUCCESS