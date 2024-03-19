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
        lib_dir = os.path.expanduser(os.path.expandvars('$HOME/AnyLog-Network/source/generic/c_code'))
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
