"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
from edge_lake.generic import interpreter


# ============================================================================================
# Stores files into buckets (i.e. Filecoin / S3)
# ============================================================================================


# ============================================================================================
# Bucket Create ...
# Example: bucket create where provider = akave and name = my_bucket
# ============================================================================================

def bucket_create(status, io_buff_in, cmd_words, trace):


    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"provider": ("str", True, False, True),
                "name":     ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    provider = interpreter.get_one_value(conditions, "provider")



