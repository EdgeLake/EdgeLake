"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.blockchain.ethereum as ethereum
import edge_lake.blockchain.hyperledger as hyperledger

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter

bconnect_ = {}      # Connection info to the blockchain platforms
local_txn_counter_ = 0      # A counter for the number of updates done to the local ledger file

# ==================================================================
# Get the local transaction counter (number of updates to the local copy of the ledger)
# ==================================================================
def get_local_txn_counter():
    global local_txn_counter_  # A counter for the number of updates done to the local ledger file

    # It indicates how many updates were done to the local file
    # It is used by the synchronizer to determine if blockchain read is needed (to sync local state with global state)
    return local_txn_counter_

# ==================================================================
# Count updates to the local copy of the ledger
# ==================================================================
def count_local_txn():
    global local_txn_counter_  # A counter for the number of updates done to the local ledger file
    local_txn_counter_ += 1

# =======================================================================================================================
# Connect to a blockchain platform
# Return True if object was connected
'''
<blockchain connect ethereum where provider = "https://rinkeby.infura.io/v3/45e96d7ac85c4caab102b84e13e795a1" and
		contract = "0x3899bED34d9e3032fb0d544CB76bA7F752Bf5EbE" and
		private_key = "a4caa21209188ef5c3be6ee4f73c12a8c306a917c969638fb69f164b0ed95380" and 
		public_key = "0x982AF5e1589f1486b4bA17aFB6eb940aAeBBdfdB" and 
		gas_read = 3000000  and
		gas_write = 3000000>
'''
# =======================================================================================================================
def blockchain_connect(status, platform_name, conditions):

    global bconnect_

    provider = interpreter.get_one_value_or_default(conditions, "provider", "")
    ret_val = process_status.BLOCKCHAIN_not_recognized

    if platform_name == "ethereum":
        if not provider:
            status.add_error("Missing providers in 'blockchain connect to ethereum' call")
            ret_val = process_status.ERR_command_struct
        else:
            error_info = "using URL: '%s'" % provider   # Display provider on the error message
            ret_val, bobject = ethereum.connect(status, provider) # Init an AnyLog Object with the Ethereum info and calls
    elif platform_name == "hyperledger":
        error_info = "using config file: '%s'" % interpreter.get_one_value_or_default(conditions, "config_file", "")
        ret_val, bobject = hyperledger.connect(status, provider)  # Init an AnyLog Object with the Ethereum info and calls
    elif platform_name == "optimism":
        if not provider:
            status.add_error("Missing providers in 'blockchain connect to optimism' call")
            ret_val = process_status.ERR_command_struct
        else:
            error_info = "using URL: '%s'" % provider   # Display provider on the error message
            ret_val, bobject = ethereum.connect(status, provider) # Init an AnyLog Object with the Ethereum info and calls

    if not ret_val:
        # was connected

        ret_val =  bobject.make_connection(status, conditions)  # Connect to the Ethereume node

        if not ret_val:
            bconnect_[platform_name] = bobject

            if bobject.is_connected():
                ret_val = process_status.SUCCESS
            else:
                ret_val = process_status.Connection_error
        else:
            ret_val = process_status.Connection_error

        if ret_val:
            message = "Failed to connect to blockchain platform: '%s' %s" % (platform_name, error_info)
            status.add_error(message)
            utils_print.output_box(message)
        else:
            # Account info can be called from connect or with a separate call
            ret_val = set_account_info(status, platform_name, conditions)

    return ret_val
# ------------------------------------------
# Set parameters for the platform
# ------------------------------------------
def set_account_info(status, platform_name, conditions):

    global bconnect_

    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]

        ret_val = platform.set_account_info(status, conditions)

    return ret_val

# ------------------------------------------
# Create a new account - return the keys
# ------------------------------------------
def create_account(status, platform_name):

    ethereum_keys = None
    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]
        if platform.is_connected():
            ret_val, ethereum_keys = platform.create_account(status)
        else:
            ret_val = process_status.Connection_error

    return [ret_val, ethereum_keys]

# ------------------------------------------
# Get The Blockchain platforms being used
# ------------------------------------------
def get_platforms(status, io_buff_in, cmd_words, trace):

    global bconnect_

    platforms_used = []

    for platform_name, platform in bconnect_.items():
        chain_id = 0
        if platform:
            connect_status = platform.is_connected()
            balance = platform.get_balance()
            chain_id = platform.get_chain_id()

            if platform_name == "ethereum" or platform_name == "optimism":

                connect_str = platform.get_provider()
                public_key = platform.get_public_key()
                contract = platform.get_contract_addr()
            elif platform_name == "hyperledger":
                connect_str = platform.get_config_file()
                public_key = platform.get_certificate_dir()
                contract = platform.get_contract_name()
        else:
            connect_status = False
            connect_str = ""
            balance = "Not Available"
            public_key = "Not Available"

        platforms_used.append((platform_name, str(connect_status), balance, chain_id, "Public Key", public_key))
        platforms_used.append(("", "", "", "", "Contract", contract))
        platforms_used.append(("", "", "", "", "URL/Config", connect_str))

    if len(platforms_used):
        reply = utils_print.output_nested_lists(platforms_used, "Blockchains connected", ["Name", "Active", "Balance", "Chain ID", "Key Types", "Keys"], True, "")
    else:
        reply = "No connections to blockchain platforms"

    return [process_status.SUCCESS, reply]

# =======================================================================================================================
# Deploy a contract
# =======================================================================================================================
def deploy_contract(status, platform_name, public_key):

    global bconnect_

    contract_id = None
    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]
        if platform.is_connected():
            ret_val, contract_id = platform.deploy_contract(status, public_key)
        else:
            ret_val = process_status.Connection_error

    return [ret_val, contract_id]

# =======================================================================================================================
# Write the JSON file to the blockchain
# Example command: blockchain commit to ethereum !json_script
# =======================================================================================================================
def blockchain_commit(status, platform_name, policy_id, policy, trace):

    global bconnect_

    tx_receipt = None

    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]
        if platform.is_connected():
            ret_val, reply = platform.put_policies(status, policy_id, policy)
        else:
            ret_val = process_status.Connection_error

    return [ret_val, tx_receipt]

# =======================================================================================================================
# Update the JSON file to the blockchain
# Example command: blockchain update to ethereum !policy_id !json_policy
# =======================================================================================================================
def blockchain_update(status, platform_name, policy_id, policy, trace):

    global bconnect_

    tx_receipt = None

    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]
        if platform.is_connected():
            ret_val, reply = platform.update_policies(status, policy_id, policy)
        else:
            ret_val = process_status.Connection_error

    return [ret_val, tx_receipt]

# =======================================================================================================================
# delete a policy from the blockchain
# =======================================================================================================================
def blockchain_delete(status, platform_name, policy_id, trace):

    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized
    else:
        platform = bconnect_[platform_name]
        if platform.is_connected():
            ret_val, reply = platform.delete_policies(status, policy_id)
        else:
            ret_val = process_status.Connection_error

    return ret_val

# =======================================================================================================================
# Return the number of transactions for an address
# =======================================================================================================================
def get_txn_count(status, platform_name, addr_type):
    '''
    platform_name - i.e. ethereum
    addr_type - "public" for public key or "contract" for contract address
    '''
    txn_count = 0
    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized

    else:
        ret_val = process_status.SUCCESS
        platform = bconnect_[platform_name]
        if platform.is_connected():
            if addr_type == "public":
                address = platform.get_public_key()
            elif addr_type == "contract":
                address = platform.get_contract_addr()
            else:
                ret_val = process_status.Wrong_address
            if not ret_val:
                ret_val, txn_count = platform.get_trn_count(status, address)
        else:
            ret_val = process_status.Connection_error

    return [ret_val, txn_count]


# =======================================================================================================================
# Checkout the JSON data from the blockchain
# Example command: blockchain checkout from ethereum !file_name
# =======================================================================================================================
def blockchain_checkout(status, platform_name, trace):

    # Pull from blockchain and write the JSON data to the file on return data to stdout]
    reply_data = ""
    if platform_name not in bconnect_:
        status.add_error("Blockchain platform '%s' is not recognized or not connected" % platform_name)
        ret_val = process_status.BLOCKCHAIN_not_recognized

    else:
        platform = bconnect_[platform_name]

        if platform.is_connected():
            ret_val, source_data = platform.get_policies(status, True)

            if not ret_val:
                # Validate the data and create a string
                if isinstance(source_data, list):
                    reply_data = list_to_policies(status, source_data)
        else:
            status.add_error(f"Blockchain platform '{platform_name}' not connected")
            ret_val = process_status.Connection_error

    return [ret_val, reply_data]

# =======================================================================================================================
# is connected platform
# =======================================================================================================================
def is_connected(platform_name):

    if not platform_name in bconnect_:
        ret_val = False     # Not declared
    else:
        platform = bconnect_[platform_name]
        if platform:
            ret_val = platform.is_connected()
        else:
            ret_val = False

    return ret_val

# =======================================================================================================================
# Transform a list of entries from the blockchain contract to a list of policies.
# Remove unused policies and duplicates
# =======================================================================================================================
def list_to_policies(status, source_data):

    discarded = {}          # A dictionary with the list of policies to discard

    policies = {}           # Valid policies

    for entry in source_data:
        if isinstance(entry, dict):
            policy = entry          # Example use case - Hyperledger
            if "policy" in policy:
                policy = policy["policy"]       # Hyperledger policies are stored with the key "policy"
        elif  isinstance(entry, str):
            policy = utils_json.str_to_json(entry) # Example use case - Ethereum
        else:
            continue        # A policy in a wrong structure was added
        if policy and isinstance(policy,dict) and len(policy) == 1:
            # Root key is the policy type
            policy_type = utils_json.get_policy_type(policy)
            if not isinstance(policy[policy_type], dict):
                continue        # Wrong policy struct

            if "id" in policy[policy_type]:     # Ignore policies without ID
                id = policy[policy_type]["id"]
                if id in policies:
                    # Keep the latest
                    existing_policy = policies[id]
                    if utils_json.compare_policies_dates(policy, policy_type, existing_policy, None) == 1:
                        policies[id] = policy       # Keep with the latest date
                else:
                    policies[id] = policy

                if policy_type == "discard":        # Policy to Ignore
                    # Discard policy needs to have 2 attributes:
                    # 1) "policy" - with the id to the policy to discard
                    # 2) "status" - "active" or "off"
                    if "policy" in policy["discard"] and "status" in policy["discard"]:
                        discard_id = policy["discard"]["policy"]
                        if discard_id in discarded:
                            # Second reference to the policy - keep the latest
                            existing_policy = discarded[discard_id]
                            if utils_json.compare_policies_dates(policy, "discard", existing_policy, "discard") == 1:
                                discarded[discard_id] = policy  # Keep with the latest date
                        else:
                            discarded[discard_id] = policy


    # Remove discarded policies
    for discard_id, discard_policy in discarded.items():
        if discard_policy["discard"]["status"] == "active":
            del policies[discard_id]

    # Return a list of strings

    all_policies = ""
    for entry in policies.values():
        policy_str = utils_json.to_string(entry)
        if policy_str:
            all_policies += (policy_str + "\n")               # Update the string data

    return all_policies







