"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# This library provides extenal and internal methods that can be used in the processing as a parameter

import importlib
import sys

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter

func_registry_ = {}

params_registry_ = {}

# -----------------------------------------------------------------------
# Get the function from the registry
# -----------------------------------------------------------------------
def get_func(import_name):
    global func_registry_
    return func_registry_[import_name]["func_reference"] if import_name in func_registry_ else None

# -----------------------------------------------------------------------
# Get the params from the registry
# -----------------------------------------------------------------------
def get_params(import_name):
    global params_registry_

    if import_name in params_registry_:
        params_values = {}
        name_params = params_registry_[import_name]
        for param_name, param_type_value in name_params.items():
            params_values[param_name] = param_type_value[1]     # Skip the type
    else:
        params_values = None
    return params_values


# -----------------------------------------------------------------------
# Load and name an external function
# import function where name = yolo_detect and lib = external_lib.frame_modeling.yolo_detection and class = YoloDetection and method = collect_detections
# -----------------------------------------------------------------------
def import_function(status, io_buff_in, cmd_words, trace):

    #                          Must     Add      Is
    #                          exists   Counter  Unique`

    global func_registry_
    keywords = {"import_name": ("str", True, False, True),
                "lib": ("str", True, False, True),
                "class": ("str", False, False, True),
                "method": ("str", True, True, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        return ret_val

    import_name = interpreter.get_one_value(conditions, "import_name")
    lib_name = interpreter.get_one_value(conditions, "lib")
    class_name = interpreter.get_one_value(conditions, "class")
    method_name = interpreter.get_one_value(conditions, "method")


    try:
        # 1️⃣ Import the module dynamically
        mod = importlib.import_module(lib_name)

        if class_name:
            # 2️⃣ Get the class from the module
            cls = getattr(mod, class_name)

            # 3️⃣ Get the method from the class
            func = getattr(cls, method_name)
        else:
            # 2️⃣ Get the method from the module
            func = getattr(mod, method_name)

    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to import method '{method_name}': {value}")
        ret_val = process_status.Failed_to_import_lib
    else:
        func_registry_[import_name] = {
            "lib" : lib_name,
            "class" : class_name,
            "method" : method_name,
            "func_reference" : func,
        }
        ret_val = process_status.SUCCESS

    return ret_val


# -----------------------------------------------------------------------
# set params to the existing imported functions
# set function params where import_name = [param name] and param_name = [param name] and param_value = [param value]
# -----------------------------------------------------------------------
def set_func_params(status, io_buff_in, cmd_words, trace):
   #                          Must     Add      Is
    #                          exists   Counter  Unique`

    global func_registry_
    global params_registry_
    keywords = {"import_name": ("str", True, False, True),
                "param_name": ("str", True, False, True),
                "param_type": ("str", False, False, True),      # Default is string
                "param_value": ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    import_name = interpreter.get_one_value(conditions, "import_name")
    if not import_name in func_registry_:
        status.add_error(f"Import name '{import_name}' is not registered - use 'import function' command to register the function")
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS
        param_name = interpreter.get_one_value(conditions, "param_name")
        param_type = interpreter.get_one_value_or_default(conditions, "param_type", "str")
        param_value = interpreter.get_one_value(conditions, "param_value")


        saved_val = utils_data.convert_value(param_value, param_type) if param_type != "str" else param_value


        if import_name in params_registry_:
            # ADd a new param
            params_registry_[import_name][param_name] = (param_type, saved_val)
        else:
            params_registry_[import_name] = {param_name: (param_type, saved_val)}   # First param

    return ret_val
# -----------------------------------------------------------------------
# Reset the function params
# reset function params where import_name = yolo_detect
# -----------------------------------------------------------------------
def reset_params(status, io_buff_in, cmd_words, trace):

    global params_registry_

    if not utils_data.test_words(cmd_words, 3, ["where", "import_name", "="]):
        ret_val = process_status.ERR_command_struct
    else:
        import_name = cmd_words[6]
        if not import_name in params_registry_:
            status.add_error(f"Reset params command failed: {import_name} not in registry")
            ret_val = process_status.ERR_command_struct
        else:
            del params_registry_[import_name]
            ret_val = process_status.SUCCESS
    return ret_val

# -----------------------------------------------------------------------
# Get the list of imported functions
# get imported functions
# -----------------------------------------------------------------------
def get_imported_functions(status, io_buff_in, cmd_words, trace):

    global func_registry_

    info_str = utils_print.print_flat_dict_as_table(func_registry_, ["func_reference"], "Reference Name", "Imported Functions", True)

    return [process_status.SUCCESS, info_str]


# -----------------------------------------------------------------------
# Get the list of imported functions
# get imported functions
# -----------------------------------------------------------------------
def get_functions_params(status, io_buff_in, cmd_words, trace):

    global params_registry_

    output_table = []
    for key, params_name_value in params_registry_.items():
        import_name = key       # The name identifying the import
        for param_name, param_type_value in params_name_value.items():
            # Go over all the name - values
            output_table.append([import_name, param_name, param_type_value[0], param_type_value[1]])
            import_name = ""

    if len(output_table):
        info_str = utils_print.output_nested_lists(output_table, "Function params", ["Import Name", "Param Name", "Param Type", "Param Value"], True)
    else:
        info_str = "No Function params declared"


    return [process_status.SUCCESS, info_str]


