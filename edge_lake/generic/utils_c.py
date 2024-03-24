"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import sys
import platform
import ctypes
import ctypes.util


# ----------------------------------------------------------------
# Load C code
# https://solarianprogrammer.com/2019/07/18/python-using-c-cpp-libraries-ctypes/
# ----------------------------------------------------------------
def load_lib(lib_name):
    try:
        lib_dir = os.path.expanduser(os.path.expandvars('$HOME/EdgeLake/source/generic/c_code'))
        c_lib = ctypes.CDLL(lib_dir + "/" + lib_name)  # load a c code that does the user input

    except OSError as e:
        print("Unable to load the system C library: %s", lib_name)
        c_lib = None

    return c_lib


# ----------------------------------------------------------------
# Set the function with in the library with arguments and returned value
# ----------------------------------------------------------------
def prep_readline(c_lib):
    ret_val = True
    try:
        c_lib.al_readline.restype = ctypes.c_char_p
    except OSError as e:
        ret_val = False
    # c_lib.al_readline.argtypes = ctypes.py_object  # variables passed as immutable array. Example: dll.sum((1,2,3,4))

    return ret_val


# ----------------------------------------------------------------
# get command from user input as a string of bytes
# ----------------------------------------------------------------
def readline(c_lib):
    str_returned = c_lib.al_readline()
    return str_returned
