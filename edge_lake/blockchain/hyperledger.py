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

# Project - https://pypi.org/project/fabric-sdk-py/
# API - https://github.com/hyperledger/fabric-sdk-py
# See API examples at https://fabric-sdk-py.readthedocs.io/en/latest/tutorial.html


import sys
try:
    from py4j.java_gateway import JavaGateway
except:
    jgateway_installed = False
else:
    jgateway_installed = True

import anylog_node.blockchain.gateway as gateway
import anylog_node.generic.process_status as process_status
import anylog_node.generic.process_log as process_log
import anylog_node.generic.utils_json as utils_json
import anylog_node.generic.interpreter as interpreter

# --------------------------------------------------------------------------------------
#  Connect to Hyperledger - using the provider connection URL,
# Init an AnyLog Object with the Ethereum info and calls
# --------------------------------------------------------------------------------------
def connect(status, provider):
    '''
    provider - the URL to the Ethereum provider

    returns - SUCCESS or Error code + the AnyLog Object
    '''

    if not jgateway_installed:
        hl_connection = None
        status.add_error("Failed to connect to Hyperledger: Fabric-SDK-Py not installed")
        ret_val = process_status.Failed_to_import_lib
    else:
        try:
            hl_connection = Hyperledger(provider)   # Init an ANyLog object
        except:
            hl_connection = None
            err_msg = "Failed to connect to Hyperledger with provider: %s" % str(provider)
            status.add_error(err_msg)
            ret_val = process_status.Connection_error
        else:
            ret_val = process_status.SUCCESS

    return [ret_val, hl_connection]



class Hyperledger(gateway.BlockchainNode):
    '''
    An Object containing the connection info to Ethereum
    '''

    # -------------------------------------------------------------
    # Connect to platform
    # -------------------------------------------------------------
    def make_connection(self, status, conditions):

        if not jgateway_installed:
            ret_val = process_status.Failed_to_import_lib
        else:
            self.certificate_dir, self.config_file, network_name, contract_name = interpreter.get_multiple_values(conditions,
                                                                          ["certificate_dir", "config_file", "network", "contract"],
                                                                          [None, None, None, None])

            if not self.certificate_dir or not self.config_file or not network_name or not contract_name:
                if not self.certificate_dir:
                    missing_value = "certificate_dir"
                elif not self.config_file:
                    missing_value = "config_file"
                elif not network_name:
                    missing_value = "network"
                else:
                    missing_value = "contract"

                status.add_error("Hyperledger failed to connect: missing value for: %s" % missing_value)
                ret_val = process_status.ERR_command_struct

            else:
                try:
                    # Do the connection to the node
                    gateway = JavaGateway()
                    self.connection = gateway.entry_point
                    connection_flag = self.connection.set_connection(self.certificate_dir, self.config_file, network_name, contract_name)  # Flag that a working connection was established
                except:
                    errno, value = sys.exc_info()[:2]
                    err_msg = "Hyperledger failed to connect to provider with error: %s" % (str(value))
                    status.add_error(err_msg)
                    self.connected = False
                    ret_val = process_status.Connection_error
                else:
                    if connection_flag:
                        ret_val, contract_name = self.get_contract_instance()  # Get the contract name
                        if not ret_val and contract_name:
                            self.connected = True
                    else:
                        err_msg = "Hyperledger failed to connect to provider"
                        status.add_error(err_msg)
                        self.connected = False
                        ret_val = process_status.Connection_error

        return ret_val

    # -------------------------------------------------------------
    # Create sender Account
    # -------------------------------------------------------------
    def set_sender_account(self, status):
        '''
        Creates an account object from a private key.
        Returns - The account object
        '''
        try:
            self.senderAccount = None
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Hyperledger failed to associate an account with the provided private key - Error: %s" % str(value)
            status.add_error(err_msg)
            ret_val = process_status.BLOCKCHAIN_operation_failed
        else:
            ret_val = process_status.SUCCESS
        return ret_val

    # -------------------------------------------------------------
    # Create Contract Address
    # -------------------------------------------------------------
    def set_contract_address(self, status):
        try:
            self.contract_address = None    # Returns the given address with an EIP55 checksum.
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Hyperledger failed to associate a contract ti an account - Error: %s" % str(value)
            status.add_error(err_msg)
            ret_val = process_status.BLOCKCHAIN_operation_failed
        else:
            ret_val = process_status.SUCCESS
        return ret_val

    # -------------------------------------------------------------
    # Create an account
    # -------------------------------------------------------------
    def create_account(self, status):

        try:
            new_account = None
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Hyperledger failed to Create Account with error: %s" % (str(value))
            status.add_error(err_msg)
            ret_val = process_status.BLOCKCHAIN_operation_failed
        else:
            ethereum_keys = "\r\nPublic key:  %s\r\nPrivate Key: %s" % (new_account._address, new_account._private_key.hex())
            ret_val = process_status.SUCCESS

        return [ret_val, ethereum_keys]

    # -------------------------------------------------------------
    # Return the account balance
    # For new Ether - https://faucet.rinkeby.io/
    # -------------------------------------------------------------
    def get_balance(self):
        return 0
    # -------------------------------------------------------------
    # Test connection
    # 2 Conditions needs to be satisfied:
    #   a) If connection flag is set (connection was established)
    #   b) Is not disconnected - calling the Ethereum API to validate connection
    # -------------------------------------------------------------
    def is_connected(self):

        if self.connected:
            # connection was established, test it is not disconnected
            try:
                self.connected = self.connection.is_connected()  # Test that connection is valid
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Hyperledger isConnected failed: {0} : {1}".format(str(errno), str(value))
                process_log.add("Error", err_msg)

                self.connected = False


        return self.connected

    # -------------------------------------------------------------
    # Return the contract instance
    # -------------------------------------------------------------
    def get_contract_instance(self):

        try:
            self.contract_name = self.connection.get_contract_name()
            if self.contract_name:
                ret_val = process_status.SUCCESS
            else:
                err_msg = "Hyperledger contract not assigned (use: 'blockchain set account info' command)"
                process_log.add("Error", err_msg)
                ret_val = process_status.BLOCKCHAIN_contract_error
        except:
            self.contract_name = ""
            err_msg = "Hyperledger contract not assigned (use: 'blockchain set account info' command)"
            process_log.add("Error", err_msg)
            ret_val = process_status.BLOCKCHAIN_contract_error

        return [ret_val, self.contract_name]

    # -------------------------------------------------------------
    # Add data to the blockchain - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    def put_policies(self, status, policy_id, policy):

        if not self.connected:
            ret_message = "Blockchain Failure: Hyperledger platform not connected"
            status.add_error(ret_message)
            ret_val = process_status.Connection_error
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if not ret_val:
                try:
                    ret_message = self.connection.insert_policy(policy_id, policy)
                except:
                    errno, value = sys.exc_info()[:2]
                    ret_message = "Blockchain Failure: new policy not written: {0} : {1}".format(str(errno), str(value))
                    status.add_error(ret_message)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if ret_message:
                        # An error message is returned if insert failed (i.e. duplicte key error)
                        status.add_error(ret_message)
                        ret_val = process_status.BLOCKCHAIN_operation_failed
                    else:
                        ret_val = process_status.SUCCESS
            else:
                ret_message = "Blockchain Failure: Hyperledger contract not available"
                status.add_error(ret_message)

        return [ret_val, ret_message]

    # -------------------------------------------------------------
    # Delete Policy - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    def delete_policies(self, status, policy_id):

        if not self.connected:
            ret_message = "Blockchain Failure: Hyperledger platform not connected"
            status.add_error(ret_message)
            ret_val = process_status.Connection_error
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if not ret_val:
                try:
                    ret_message = self.connection.delete_policy(policy_id)
                except:
                    errno, value = sys.exc_info()[:2]
                    ret_message = "Blockchain Failure: delete policy failed: {0} : {1}".format(str(errno), str(value))
                    status.add_error(ret_message)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if ret_message:
                        # An error message is returned if insert failed (i.e. duplicte key error)
                        status.add_error(ret_message)
                        ret_val = process_status.BLOCKCHAIN_operation_failed
                    else:
                        ret_val = process_status.SUCCESS
            else:
                ret_message = "Blockchain Failure: Hyperledger contract not available"
                status.add_error(ret_message)

        return [ret_val, ret_message]

    # -------------------------------------------------------------
    # Retrieve the data from the blockchain - either data or keys
    # Extracts the pertinent logs from a transaction receipt.
    # If there are no errors, processReceipt returns a tuple of Event Log Objects, emitted from the event (e.g. myEvent), with decoded ouput.
    # -------------------------------------------------------------
    def get_policies(self, status, is_data):
        '''
        Status - user object status
        id_data - True value retrieves policies, False retrieves keys
        '''

        policies_list = None
        if not self.connected:
            ret_message = "Blockchain Failure: Hyperledger platform not connected"
            status.add_error(ret_message)
            policies_info = None
            ret_val = process_status.Connection_error
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if not ret_val:
                try:
                    policies_info = self.connection.get_all()
                except:
                    errno, value = sys.exc_info()[:2]
                    ret_message = "Blockchain Failure: Failed to retrieve policies: {0} : {1}".format(str(errno), str(value))
                    status.add_error(ret_message)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if policies_info:
                        policies_list = utils_json.str_to_list(policies_info)

                    ret_val = process_status.SUCCESS
            else:
                ret_message = "Blockchain Failure: Hyperledger contract not available"
                status.add_error(ret_message)


        return [ret_val, policies_list]

    # -------------------------------------------------------------
    # Deploy contract
    # Create a transaction, sign it locally, and then send it to your node for broadcasting, with send_raw_transaction().
    # https://support.blockdaemon.com/hc/en-us/articles/360022161812-How-To-Deploy-A-Smart-Contract-with-An-Ethereum-Node-Using-Web3py
    # -------------------------------------------------------------
    def deploy_contract(self, status, public_key):

        global abi_
        contract_address = None
        if not self.connected:
            status.add_error("Hyperledger platform not connected")
            ret_val = process_status.Connection_error
        else:
            try:
                pass
                ret_val = process_status.SUCCESS
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Ethereum deploy contract failed: {0} : {1}".format(str(errno), str(value))
                status.add_error(err_msg)
                ret_val = process_status.BLOCKCHAIN_operation_failed

        return [ret_val, contract_address]
