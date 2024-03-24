"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data
import edge_lake.cmd.member_cmd as member_cmd
from edge_lake.generic.utils_columns import get_current_utc_time
from edge_lake.generic.params import get_value_if_available

# ------------------------------------------------------------------------------------
# Code to validate that the cluster policy exists
# ------------------------------------------------------------------------------------
def validate_cluster_exists( status, cluster_id, blockchain_file ):
    test_cmd = ["blockchain", "get", "cluster", "where", "id", "=", cluster_id, "bring.count"]
    ret_val, cluster_policy = member_cmd.blockchain_get(status, test_cmd, blockchain_file, True)
    if not ret_val:
        if not cluster_policy or cluster_policy != "1":
            status.add_error(f"Cluster policy '{cluster_id}' is referenced by an Operator and missing in the metadata")
            ret_val = process_status.Missing_cluster_policy
    return ret_val

# ------------------------------------------------------------------------------------
# For these policies - use specific field values to make the ID
# ------------------------------------------------------------------------------------
hash_fields = {
    "table" : [
        "company",
        "name",
        "dbms",
    ],
    "cluster": [
        "company",
        "name",
        "parent",
        "table",
    ]

}

# ------------------------------------------------------------------------------------
# Validate and complete Policies
# ------------------------------------------------------------------------------------
cluster_table_attr = [
    # attr name * attr type * child attributes * default value * must exists *  is Unique * validation code
    ("dbms",        str,        None,               None,          True,         False,         None),
    ("name",        str,        None,               None,          True,         False,         None),
    ("status",      str,        None,               "active",      True,         False,         None),
]

operator_attr = [
    ("company", str, None, None, True, False, None),
    ("name", str, None, None, True, False, None),
    ("ip", str, None, None, True, False, None),
    ("port", int, None, None, True, False, None),
    ("member", int, None, None, False, False, None),
    ("cluster", str, None, None, False, False, validate_cluster_exists),
]

cluster_attr = [
    ("status", str, None, "active", True, False, None),
    ("name", str, None, None, True, False, None),
    ("company", str, None, None, True, False, None),
    ("table", list, cluster_table_attr, None, False, False, None),
]

table_attr = [
    ("name", str, None, None, True, False, None),
    ("dbms", str, None, None, True, False, None),
    ("create", str, None, None, True, False, None),
]

mapping_schema_attr = [
    # attr name * attr type * child attributes * default value * must exists *  is Unique
    ("script", list,      None,              None,           False,         False, None),
    ("bring",     str,        None,              None,           False,         False, None),
    ("type",      str,        None,              None,           True,          False, None),
    ("root",      bool,       None,              None,           False,         False, None),
    ("blob",      bool,       None,              None,           False,         False, None),
    ("extension", str,        None,              None,           False,         False, None),
    ("hash",      str,        None,              None,           False,         False, None),
]

mapping_attr = [
    ("dbms",        str,    None,                   None,   True,   False,  None),
    ("table",       str,    None,                   None,   True,   False,  None),
    ("readings",    str,    None,                   None,   False,  False,  None),
    ("condition",   str,    None,                   None,   False,  False,  None),
    ("schema",      dict,   mapping_schema_attr,    None,   True,   False,  None),
    ("import",      dict,   None,                   None,   False,  False,  None),
]



member_attr = [
    # attr name * attr type * child attributes * default value * must exists *  is unique
    ("type",        str,            None,           None,           True,       False, None),
    ("public_key",  str,            None,           None,           True,       True, None),
]

permissions_attr = [
    # Permission policy determines commands and databases allowed
    # attr name * attr type * child attributes * default value * must exists *  is-unique
    ("name",        str,         None,               None,           True,        True,     None),    # A unique name to identify the policy
    ("enable",      list,        None,               None,           True,        False,    None),
    ("disable",     list,        None,               None,           False,       False,    None),
    ("databases",   list,        None,               None,           False,       False,    None),
    ("tables",      list,        None,               None,           False,       False,    None),
    ("public_key",  str,         None,               None,           True,        False,    None),
    ("signature",   str,         None,               None,           True,        False,    None),
]

assignment_attr = [
    # Connect member to permissions
    # attr name * attr type * child attributes * default value * must exists *  is-unique
    ("permissions", str,        None,               None,           True,       False, None),       # Policy id of the permissions
    ("members",     list,       None,               None,           True,       False, None),
    ("public_key",  str,        None,               None,           True,       False, None),
    ("signature",   str,        None,               None,           True,       False, None),
]

# ------------------------------------------------------------------------------------
# Unique attributes in policies
# ------------------------------------------------------------------------------------
# Values which are unique
unique_keys = {
    "operator" : [("ip", "port"), ("name",)],
    "table"    : [("dbms", "name")]
}
# ------------------------------------------------------------------------------------
# Default Values in policies
# ------------------------------------------------------------------------------------
default_values_ = {
    "master" : [
        ("ip", "!external_ip", None),
        ("internal_ip", "!ip", None),
        ("port", 32048, None),
        ("rest_port", 32049, None),
    ],
    "operator": [
        ("ip", "!external_ip", None),
        ("internal_ip", "!ip", None),
        ("port", 32148, None),
        ("rest_port", 32149, None),
    ],
    "query": [
        ("ip", "!external_ip", None),
        ("internal_ip", "!ip", None),
        ("port", 32348, None),
        ("rest_port", 32349, None),
    ],
    "publisher": [
        ("ip", "!external_ip", None),
        ("internal_ip", "!ip", None),
        ("port", 32248, None),
        ("rest_port", 32249, None),
    ],

}

# =======================================================================================================================
# These policies can be validated by calling to the method: validate_policy()
# =======================================================================================================================
policies_struct = {
               # key - values *    Unique attributes
    "table" :       (table_attr,            ("dbms","name")),
    "operator" :    (operator_attr,        ("company","name"), ("cluster","member")),
    # "cluster" : (cluster_attr,    ("name",)),  # can have multiple clusters with the same name
}
# =======================================================================================================================
# Test that policy has all components and complete missing values
# =======================================================================================================================
def process_new_policy(status, policy, blockchain_file):

    global unique_keys

    ret_val = process_status.SUCCESS

    policy_type = utils_json.get_policy_type(policy)
    if not policy_type:
        status.add_keep_error("Policy not recognized: %s" % policy)
        ret_val = process_status.ERR_wrong_json_structure
    else:
        if policy_type in new_policies:
            if not "ledger" in policy[policy_type] or policy[policy_type]["ledger"] == "local":
                # The process below validates the policy against the blockchain - this is done on the sender node.
                # Otherwise - this test on the master can trigger an error as the policy may already exists on the master.
                # In addition - in a single node deployment - the blockchain file is not relevent to consider on the master
                ret_val = new_policies[policy_type](status, policy_type, policy[policy_type], blockchain_file)

        if not ret_val:
            # Test that the policy fields are unique
            if policy_type in unique_keys:
                policy_info = policy[policy_type]
                keys_list = unique_keys[policy_type]        # A list of unique keys

                for keys in keys_list:
                    test_cmd = ["blockchain", "get", policy_type, "where"]
                    counter = 0
                    for key in keys:
                        if key not in policy_info:
                            break                           # The keys are missing - not able to validate
                        # Add the key value to the where condition
                        if counter:
                            test_cmd.append("and")
                        test_cmd.append(key)
                        test_cmd.append("=")
                        test_cmd.append(str(policy_info[key]))
                        counter += 1

                    if ret_val:
                        break

                    # if not unique - return an error
                    ret_val, cluster_policy = member_cmd.blockchain_get(status, test_cmd, blockchain_file, True)
                    if ret_val:
                        break
                    if cluster_policy and len(cluster_policy):
                        status.add_error("New Policy Error: Non unique key-values pairs: %s" % ' '.join(test_cmd))
                        ret_val = process_status.Non_unique_policy_values
                        break

    return ret_val
# =======================================================================================================================
# Test that the policy has all components and complete missing values
# =======================================================================================================================
def new_operaor_policy(status, policy_type, policy_obj, blockchain_file):

    if "cluster" in policy_obj:
        # An operator associated to a cluster can be defined only once
        ret_val = test_ip_port(status, policy_type, policy_obj, blockchain_file)

        if not ret_val:

            if "member" not in policy_obj:
                # if member id is not provided - add member id
                cluster_id = policy_obj["cluster"]

                # Generate a random member id and test that does not exists
                while 1:
                    member_id = int.from_bytes(os.urandom(1), byteorder='big', signed=False) # Generate a random member ID (up to 3 digits)
                    if not member_id:
                        continue        # 0 not allowed
                    # test that the ID was not given

                    cmd_get_operator = ["blockchain", "get", "operator", "where", "cluster", "=", cluster_id, "and", "member", "=", str(member_id)]
                    ret_val, cluster_policy = member_cmd.blockchain_get(status, cmd_get_operator, blockchain_file, True)

                    if not ret_val and len(cluster_policy):
                        continue        # This id exists in this cluster - generate another random ID for the member
                    break

                policy_obj["member"] = member_id
    else:
        # Missing cluster in the policy
        status.add_error("Operator Policy is is not associated with a cluster")
        ret_val = process_status.Wrong_policy_structure

    if not ret_val:
        ret_val = test_key_val(status, policy_type, policy_obj, operator_attr, blockchain_file)


    return ret_val

# =======================================================================================================================
# Test that the IP and Port exists in the policy and that the IP and Port are unique in the blockchain data
# =======================================================================================================================
def test_ip_port(status, policy_type, policy_obj, blockchain_file):

    ret_val, policies = get_policies_by_ip_port(status, policy_type, policy_obj, blockchain_file)
    if not ret_val and len(policies):
        if "ip" in policy_obj:
            ip_addr = policy_obj["ip"]
        else:
            ip_addr = "Not provided"
        if  "port" in policy_obj:
            port_val = str(policy_obj["port"])
        else:
            port_val = "Not provided"
        status.add_keep_error("New policy '%s' is using an IP and Port '%s:%s' which is in use by an existing policy" % (policy_type, ip_addr, port_val))
        ret_val = process_status.None_unique_ip_port
    else:
        ret_val = process_status.SUCCESS

    return ret_val

# =======================================================================================================================
# get policies by IP and Port
# =======================================================================================================================
def get_policies_by_ip_port(status, policy_type, policy_obj, blockchain_file):

    if "ip" not in policy_obj:
        status.add_keep_error("Policy '%s' is missing an IP address" % policy_type)
        policies = None
        ret_val = process_status.Wrong_policy_structure
    elif  "port" not in policy_obj:
        status.add_keep_error("Policy '%s' is missing a port value" % policy_type)
        policies = None
        ret_val = process_status.Wrong_policy_structure
    else:
        ip_addr = policy_obj["ip"]
        port_val = str(policy_obj["port"])
        # Test unique IP/Port for that type of policy
        cmd_get_policy = ["blockchain", "get", policy_type, "where", "ip", "=", ip_addr, "and", "port", "=", port_val]
        ret_val, policies = member_cmd.blockchain_get(status, cmd_get_policy, blockchain_file, True)

    return [ret_val, policies]
# =======================================================================================================================
# Test must exists key and value of a policy - test that the key exists and the type of the value
# =======================================================================================================================
def test_key_val(status, policy_type, policy_obj, root_key_val, blockchain_file):

    global policies_struct

    ret_val = process_status.SUCCESS

    if policy_type in policies_struct:
        for unique_attributes in policies_struct[policy_type][1:]:
            ret_val = test_unique_values(status, unique_attributes, policy_type, policy_obj, blockchain_file)
            if ret_val:
                break

    if not ret_val:

        for entry in root_key_val:
            if ret_val:
                break

            root_key = entry[0]
            root_val_type = entry[1]      # The data type needed
            child_key_val = entry[2]
            default_val = entry[3]
            must_exists = entry[4]
            is_unique = entry[5]         # Only 1 policy with the value is allowed
            validation_method = entry[6]

            if root_key not in policy_obj:
                if not default_val:        # No default value
                    if must_exists:
                        status.add_keep_error("Policy '%s' without a value for the key: '%s'" % (policy_type, root_key))
                        ret_val = process_status.Wrong_policy_structure
                        break
                    # Missing OPTIONAL attribute = go to the next
                    continue
                # Add the default value
                policy_obj[root_key] = default_val

            val = policy_obj[root_key]

            if is_unique:
                # Only one policy can be with the same value
                test_cmd = ["blockchain", "get", policy_type, "where", root_key, "=", val]
                ret_val, cluster_policy = member_cmd.blockchain_get(status, test_cmd, blockchain_file, True)
                if not ret_val and len(cluster_policy):
                    status.add_error(f"Error in policy type: {policy_type} - duplicate '{root_key}' value: '{val}'")
                    ret_val = process_status.Non_unique_policy_values
                    break

            if validation_method:
                # Run the method to validate
                ret_val = validation_method(status, val, blockchain_file)
                if ret_val:
                    break


            if root_val_type and not isinstance(val, root_val_type):    # Null value skips the type validation
                status.add_keep_error("Wrong data type for the attribute '%s:%s' in the policy '%s' (requires %s)" % (root_key, str(val), policy_type, str(root_val_type) ))
                ret_val = process_status.Wrong_policy_structure
                break

            if child_key_val:
                # Test the child entries
                if isinstance(val, list):
                    for child_entry in val:
                        child_policy_type = "%s[%s]" % (policy_type, root_key)
                        ret_val = test_key_val(status, child_policy_type, child_entry, child_key_val, blockchain_file)
                        if ret_val:
                            break
                elif isinstance(val, dict):
                    for child_dict_key, child_value in val.items():
                        child_policy_type = "%s[%s]" % (policy_type, child_dict_key)
                        if isinstance(child_value, dict):
                            # A dictionary with the policy instructions
                            ret_val = test_key_val(status, child_policy_type, child_value, child_key_val, blockchain_file)
                        elif isinstance(child_value, list):
                            # A list with multiple values, this is the case of multiple options with 'condition'
                            for entry in child_value:
                                if isinstance(entry, dict):
                                    ret_val = test_key_val(status, child_policy_type, entry, child_key_val, blockchain_file)
                                    if ret_val:
                                        break
                        else:
                            status.add_keep_error(f"Wrong policy format: value for attribute '{child_dict_key}' needs to be a dictionary or a list for key '{root_key}'")
                            ret_val = process_status.Wrong_policy_structure

                        if ret_val:
                            break

    return ret_val


# =======================================================================================================================
# Test that combinations of values are unique. For example, IP and Port, Company name + Operator name
# =======================================================================================================================
def test_unique_values(status, attributes, policy_type, policy_obj, blockchain_file):
    '''
    status - process object
    attributes - a list of the attr names to validate
    policy_type - the policy type tested
    policy_object - the policy values tested
    '''
    ret_val = process_status.SUCCESS

    test_cmd = ["blockchain", "get", policy_type, "where"]
    str_key = ""
    str_val = ""
    flag = False
    for entry in attributes:
        val = str(policy_obj[entry] if entry in policy_obj else "")
        if val:
            if flag:
                test_cmd.append("and")
                str_key += (".")
                str_val += (".")
            else:
                flag = True         # First attribute is being added
            test_cmd.append(entry)
            test_cmd.append("=")
            str_key += entry
            str_val += val
            test_cmd.append(val)

    if flag:
        ret_val, cluster_policy = member_cmd.blockchain_get(status, test_cmd, blockchain_file, True)
        if not ret_val and len(cluster_policy):
            status.add_error(f"Error in policy type: {policy_type} - duplicate '{str_key}' value: '{str_val}'")
            ret_val = process_status.Non_unique_policy_values
    return ret_val
# =======================================================================================================================
# Test that the policy has all components and complete missing values
# =======================================================================================================================
def new_mapping_policy(status, policy_type, policy_obj, blockchain_file):

    ret_val = test_key_val(status, policy_type, policy_obj, mapping_attr, blockchain_file)

    # Test specific values in the schema
    if not ret_val:
        if "schema" in policy_obj:
            schema = policy_obj["schema"]
            for col_name, type_info in schema.items():
                if isinstance(type_info, dict):
                    # Test data type of default value in policy
                    if "default" in type_info:
                        default_value = type_info["default"]
                        column_type = type_info["type"] # the data type assigned to the column
                        if (column_type == "string" and not isinstance(default_value, str))\
                            or (column_type == "int" and not isinstance(default_value, int))\
                            or (column_type == "bool" and not isinstance(default_value, bool)):
                            status.add_error(f"Default value ('{default_value}') for column '{col_name}' is not '{column_type}'")
                            ret_val = process_status.Wrong_policy_structure
                            break
                    # Test the script by compiling to see that no errors returned
                    if "script" in type_info:
                        non_compiled = type_info["script"]  # A list with commands
                        policy_id = policy_obj["id"] if "id" in policy_obj else "Not Determined"
                        ret_val, compiled_list = member_cmd.compile_commands(status, non_compiled, policy_id)   # Compile the script to find an error
                        if ret_val:
                            break
        if not ret_val:
            # Test Import attribute
            if "import" in policy_obj:
                import_dict = policy_obj["import"]
                for policy_id in import_dict.values():
                    if not isinstance(policy_id, str) or not len(policy_id):
                        status.add_error("Wrong policy ID in import stmt: '%s'" % str(policy_id))
                        ret_val = process_status.Wrong_policy_structure
                    else:
                        ret_val, policy_obj = get_policy_by_id(status, "mapping", policy_id)
                        if ret_val:
                            status.add_error("Failed to import policy: '%s'" % str(policy_id))
                            break

    return ret_val


# =======================================================================================================================
# Get policy by ID
# =======================================================================================================================
def get_policy_by_id(status, policy_type, policy_id):

    get_policy = None
    get_cmd = ["blockchain", "get", "*", "where", "id", "=", policy_id]

    ret_val, policy_list = member_cmd.blockchain_get(status, get_cmd, None, True)

    if not ret_val:
        if not policy_list or not len(policy_list):
            status.add_error(f"Policy Error: '{policy_type}' policy is not available: (policy id: '{policy_id}')")
            ret_val = process_status.Needed_policy_not_available
        elif len(policy_list) != 1:
            status.add_error(f"Policy Error: Duplicate '{policy_type}' policy: (policy id: '{policy_id}'")
            ret_val = process_status.Non_unique_policy
        else:
            get_policy = policy_list[0]
            if not policy_type in get_policy:
                status.add_error(f"Wrong policy type - the policy in not '{policy_type}' policy (policy id: {policy_id})")
                ret_val = process_status.Needed_policy_not_available

    return [ret_val, get_policy]

# =======================================================================================================================
# Test that the policy has all components and complete missing values
# =======================================================================================================================
def new_member_policy(status, policy_type, policy_obj, blockchain_file):

    ret_val = test_key_val(status, policy_type, policy_obj, member_attr, blockchain_file)
    if not ret_val:
        # test that no duplicate "root"
        if policy_obj["type"] == "root":
            test_cmd = ["blockchain", "get", "member", "where", "type", "=", "root"]
            ret_val, cluster_policy = member_cmd.blockchain_get(status, test_cmd, blockchain_file, True)
            if not ret_val and len(cluster_policy):
                status.add_error("Duplicate member policy: root member already exists")
                ret_val = process_status.Non_unique_policy_values

    return ret_val

# =======================================================================================================================
# Test that the policy has all components and complete missing values
# Permission policy determines commands and databases allowed
# =======================================================================================================================
def new_permission_policy(status, policy_type, policy_obj, blockchain_file):

    ret_val = test_key_val(status, policy_type, policy_obj, permissions_attr, blockchain_file)

    return ret_val

# =======================================================================================================================
# Test that the policy has all components and complete missing values
# Connect member to permissions
# =======================================================================================================================
def new_assignment_policy(status, policy_type, policy_obj, blockchain_file):
    ret_val = test_key_val(status, policy_type, policy_obj, assignment_attr, blockchain_file)

    return ret_val

# =======================================================================================================================
# Test that the policy has all components and complete missing values
# =======================================================================================================================
def new_cluster_policy(status, policy_type, policy_obj, blockchain_file):

    ret_val = test_key_val(status, policy_type, policy_obj, cluster_attr, blockchain_file)

    if not ret_val:
        # test duplicate table.dbms.name
        tables_dict = {}       # A dictionary to maintain the table names to detect duplicates
        if "table" in policy_obj:
            # test that there are no duplicate definitions for the same table
            tables_list = policy_obj["table"]
            for table in tables_list:
                dbms_name = table["dbms"]
                table_name = table["name"]
                table_key = dbms_name + '.' + table_name
                if table_key in tables_dict:
                    status.add_keep_error("Cluster policy includes duplicate definitions for dbms '%s' and table '%s'" % (dbms_name, table_name))
                    ret_val = process_status.Wrong_policy_structure
                    break
                tables_dict[table_key] = 1

    if not ret_val:
        # test Unique policy name
        name = policy_obj["name"]
        cmd_get_cluster = ["blockchain", "get", "cluster", "where", "name", "=", name]
        ret_val, existing_policy = member_cmd.blockchain_get(status, cmd_get_cluster, blockchain_file, True)
        if not ret_val:
            if "parent" in policy_obj:
                # This policy is an extension of an existing policy - need to maintain the same name as the parent
                if not len(existing_policy):
                    status.add_keep_error("Cluster policy error: parent name '%s' does not match a an existing policy" % name)
                    ret_val = process_status.Wrong_policy_structure
                else:
                    ret_val = new_cluster_child_policy(status, policy_obj, existing_policy)
            else:
                # New Policy - need to have a unique name
                if len(existing_policy):
                    status.add_keep_error("Non-unique 'name' for a new cluster policy: '%s'" % name)
                    ret_val = process_status.Wrong_policy_structure

    return ret_val

# =======================================================================================================================
# This policy extends an existing Cluster Policy
# =======================================================================================================================
def new_cluster_child_policy(status, policy_obj, existing_policies):

    parent_id = policy_obj["parent"]        # The ID of the parent company
    company_name = policy_obj["company"]        # The ID of the parent company
    parent_exists = False
    for policy in existing_policies:        # Find the parent policy
        try:
            id =  policy["cluster"]["id"]
        except:
            id = ""
        else:
            if id == parent_id:
                parent_exists = True
                break
    if not parent_exists:
        status.add_keep_error("Cluster policy error: Missing parent attribute with name: '%s' and ID: '%s'" % (policy_obj["name"], parent_id))
        ret_val = process_status.Missing_parent_policy
    else:
        # Test the same company name
        if not "company" in policy["cluster"] or policy["cluster"]["company"] != company_name:
            status.add_keep_error("Cluster policy error: Different company name for parent and child '%s' policies" % policy_obj["name"])
            ret_val = process_status.Missing_parent_policy
        else:
            ret_val = process_status.SUCCESS
    return ret_val

# =======================================================================================================================
# Per Policy methods
# =======================================================================================================================
new_policies = {          # The process to execute when a new policy is added
    "operator" : new_operaor_policy,
    "cluster": new_cluster_policy,
    "mapping" : new_mapping_policy,
    "member" : new_member_policy,
    "permissions" : new_permission_policy,      # Permission policy determines commands and databases allowed
    "assignment" :  new_assignment_policy,      # Connect member to permissions
}

# =======================================================================================================================
# If a JSON structure has no ID, add ID
# Note: ID is calculated without date such that 2 objects with the same data can be compared by their hash ignoring the date.
# =======================================================================================================================
def add_json_id_date(json_dictionary: dict):

    global hash_fields

    if not isinstance(json_dictionary,dict) or len(json_dictionary) != 1:
        # Must have only a single key to the second layer object
        return process_status.Wrong_policy_structure

    key = next(iter(json_dictionary))

    json_object = json_dictionary[key]

    if not isinstance(json_object, dict):
        return process_status.Wrong_policy_structure

    if "id" in json_object:
        with_id = True
    else:
        with_id = False


    # Add date if missing
    if "date" in json_object:
        if with_id:
            return process_status.SUCCESS       # id and date are in the object
        policy_date = json_object["date"]
        del json_object["date"]                 # delete the date such that the hash can be calculated
    else:
        policy_date = get_current_utc_time()

    if "ledger" in json_object:
        # the "ledger" value is changed from "local" to "global" and is not included in the calculation of the ID
        policy_ledger = json_object["ledger"]
        del json_object["ledger"]
    else:
        policy_ledger = None

    if not with_id:
        # Calculate the hash value to make an id for the object
        if key in hash_fields:
            # Only named fields determine the ID
            id_fields = hash_fields[key]
            json_string = key + ':'
            for entry in id_fields:
                if entry in json_object:
                    json_string += entry + ':' + str(json_object[entry]) + ':'
        else:
            json_string = str(json_dictionary)

        id_string = utils_data.get_string_hash('md5', json_string, None)
        json_object["id"] = id_string

    json_object["date"] = policy_date           # Add the date

    if policy_ledger:
        json_object["ledger"] = policy_ledger  # Add the date

    return process_status.SUCCESS

# =======================================================================================================================
# Validate that the needed attributes exists in the policy
# =======================================================================================================================
def validate_policy(status, policy, unique_dict):
    '''
    unique_dict - a dictionary to test duplicate policies in the same file
    '''

    global policies_struct      # defines the policies that can be validated

    policy_type = utils_json.get_policy_type(policy)
    if not policy_type:
        status.add_keep_error("Policy not recognized: %s" % policy)
        ret_val = process_status.Wrong_policy_structure
    else:
        ret_val = process_status.SUCCESS
        if policy_type in policies_struct:
            structure = policies_struct[policy_type][0]    # get the struct of the policies
            policy_values = policy[policy_type]
            if not isinstance(policy_values, dict):
                status.add_keep_error("Policy '%s' has no body" % policy_type)
                ret_val = process_status.Wrong_policy_structure
            else:
                for entry in structure:
                    # go over the policy values and test if valid
                    key = entry[0]
                    if key in policy_values:
                        value = policy_values[key]
                        if type(value) != entry[1]:
                            # wrong data type
                            status.add_keep_error("Policy '%s' is with the wrong data type in key: %s " % (policy_type, key))
                            ret_val = process_status.Wrong_policy_structure
                    elif entry[4]:
                        # Entry that must exists is missing
                        status.add_keep_error("Policy '%s' is missing a value for the key: %s " % (policy_type, key))
                        ret_val = process_status.Wrong_policy_structure

                    if ret_val:
                        break   # Stop after firs error


            if not ret_val and unique_dict != None:
                # test that the policy is unique in the file
                for unique_attributes in  policies_struct[policy_type][1:]:
                    # Make a key using these attribute values of the policy
                    unique_key = policy_type
                    for attribute in unique_attributes:
                        if attribute not in policy_values:
                            policy_id = policy_values["id"] if "id" in policy_values else ""
                            status.add_keep_error("Policy '%s' error: Atribute '%s' is missing in policy type: '%s'" % (policy_id, attribute, policy_type))
                            ret_val = process_status.Wrong_policy_structure
                            break
                        value_segment = str(policy_values[attribute])
                        unique_key += ('.' + value_segment)
                    if ret_val:
                        break
                    if unique_key in unique_dict:
                        status.add_keep_error("Duplicate definitions for policy: %s " % unique_key)
                        ret_val = process_status.Non_unique_policy_values
                        break
                    else:
                        unique_dict[unique_key] = True      # Place unique key in dictionary

    return ret_val
# =======================================================================================================================
# Compare 2 policies - Same type + same ID - if policies structure is wrong - return not equal
# =======================================================================================================================
def compare_policies(policy_1, policy_2):
    '''
    Compare 2 policies: If IDs are the same - return True, else - Return False
    '''
    policy_type = utils_json.get_policy_type(policy_1)
    if not policy_type:
        ret_val = False
    else:
        try:
            ret_val = policy_1[policy_type]["id"] == policy_2[policy_type]["id"]
        except:
            ret_val = False
    return ret_val
# =======================================================================================================================
# Add the default values to a policy
# =======================================================================================================================
def add_policy_defaults(status, policy_type, policy_inner):
    '''
    policy_type - Master, Operator, Query, Publisher
    policy_inner - the key value pairs of the policy
    company_name - taken from the license key or Master
    '''
    ret_val = process_status.SUCCESS
    if policy_type in default_values_:
        new_values = default_values_[policy_type]
        for entry in new_values:
            key = entry[0]
            if not key in policy_inner:
                # Add default if key is missing in the policy

                value = entry[1]
                if entry[2]:
                    # function to execute
                    ret_val, value = entry[2](status, policy_type, value)    # For example: get_unused_port
                    if ret_val:
                        break
                if isinstance(value,str):
                    str_value = get_value_if_available(value)
                    policy_inner[key] = str_value.lower()
                else:
                    # if int
                    policy_inner[key] = value

    return ret_val