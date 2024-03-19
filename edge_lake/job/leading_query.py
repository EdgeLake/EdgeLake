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
import anylog_node.generic.utils_columns as utils_columns


class LeadingQuery():

    def __init__(self):
        self.leading_query = ""  # the SQL of the leading query
        self.function = ""  # The function on the result
        self.time_unit = ""  # time interval
        self.units = 0  # number of units
        self.projection_func = utils_columns.ProjectionFunctions()

    # =======================================================================================================================
    # Returns the function processed
    # =======================================================================================================================
    def get_function(self):
        return self.function

    # =======================================================================================================================
    # Returns the results to apply to the next query
    # =======================================================================================================================
    def get_results(self):

        results = []
        if self.function == "period":
            self.calculate_period_results(results)

        return results

    # =======================================================================================================================
    # Organize the result of processing a PERIOD function.
    # A period function returns start date and end date
    # =======================================================================================================================
    def calculate_period_results(self, results):

        function_array = self.projection_func.get_functions()  # get an array with the functions
        if len(function_array):
            value_array = function_array[0][1].get_results()
            end_date = value_array[0]
            # get the start date by subtracting time_units X units
            start_date = utils_columns.get_start_date(end_date, self.time_unit, self.units)

            results.append(start_date)
            results.append(end_date)

    # =======================================================================================================================
    # Returns the functions array if the query executed can process with AnyLog native functions (like MIN, NAX) - without a local database
    # =======================================================================================================================
    def get_projection_functions(self):
        return self.projection_func  # the functions to execute

    # -------------------------------------------------------------
    # Save the leading query
    # -------------------------------------------------------------
    def set_leading_query(self, query):
        self.leading_query = query

    # -------------------------------------------------------------
    # Get the leading query
    # -------------------------------------------------------------
    def get_leading_query(self):
        return self.leading_query

    # -------------------------------------------------------------
    # set period for the leading query function
    # -------------------------------------------------------------
    def set_period_function(self, time_unit, units):
        self.function = "period"
        self.time_unit = time_unit
        self.units = units
