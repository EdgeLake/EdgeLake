"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.utils_print as utils_print
import edge_lake.blockchain.blockchain as blockchain
import edge_lake.tcpip.net_utils as net_utils
from edge_lake.generic.utils_columns import get_current_utc_time, get_current_time_in_sec, utc_to_timezone

# ----------------------------------------------------------
# A metadata layer based on the blockchain data
# Providing efficient info on data distribution and physical servers
# These structures are updated for the tables and servers that are of interest on this node.
# ----------------------------------------------------------

metadata_mutex = utils_threads.ReadWriteLock()  # Read/Write mutex to avoid multiple threads updating the structure at the same time

table_to_cluster_ = {}  # Mapping from dbms + table to the clusters supporting the distributions
# company -> dbms -> table -> distribution ID + cluster supporting the distribution ID

cluster_to_servers_ = {}  # Mapping from cluster to the individual servers

ip_port_to_operator_ = {}   # Mapping from IP and port to the cluster info
# For every IP+ Port, shows a link to the Operator info and the status of the operator
# ip_port_to_operator_ is an independent structure that is not deleted when metadata is refreshed

dbms_to_company_ = {}        # Mapping databases to companies. This struct used by a publisher to determine the company by the dbms name

member_status_ = {}      # For every member in the cluster - the status of the first and last row updated in the TSD table

load_counter_ = 0       # counter for every time a new version is loaded to RAM

active_flag_ = False

member_ = 0         # The member ID of this node in the cluster
operator_name_ = ""
cluster_id_ = "0"   # The member ID of this node in the cluster
company_name_ = ""       # Name of the company on the cluster instance
cluster_name_  = ""      # Name of the cluster on the cluster instance

operator_policy_id_ = "0"   # When the node is declared as an operator - the policy id representing the node as an operator

update_time_ = 0        # The time the metadata is updated

# ----------------------------------------------------------
# Return the time the metadata was updated
# ----------------------------------------------------------
def get_update_time():
    global update_time_
    return update_time_
# ----------------------------------------------------------
# Return the member id
# ----------------------------------------------------------
def get_member_id():
    global member_
    return member_

# ----------------------------------------------------------
# Return the cluster id
# ----------------------------------------------------------
def get_cluster_id():
    global cluster_id_
    return cluster_id_

# ----------------------------------------------------------
# Return cluster name
# ----------------------------------------------------------
def get_cluster_name():
    global cluster_name_
    return cluster_name_
# ----------------------------------------------------------
# return the company name
# ----------------------------------------------------------
def get_company_name():
    global company_name_
    return company_name_

# ----------------------------------------------------------
# When metadata is loaded, active_flag is set tto True
# ----------------------------------------------------------
def is_active():
    global active_flag_
    return active_flag_
# ----------------------------------------------------------
# Get the ID of this node in the cluster
# ----------------------------------------------------------
def get_node_member_id():
    global member_
    return member_
# ----------------------------------------------------------
# Get the Operator Policy ID
# ----------------------------------------------------------
def get_node_policy_id():
    global operator_policy_id_
    return operator_policy_id_
# ----------------------------------------------------------
# Get the name of the operator assigned to this node
# ----------------------------------------------------------
def get_node_operator_name():
    global operator_name_
    return operator_name_

# ----------------------------------------------------------
# Test if the tsd table is the current node -
# compare the number on the TSD table bame to the member id
# ----------------------------------------------------------
def is_current_node(tsd_name:str):
    global member_
    ret_val = False
    if len(tsd_name) > 4:       # need to be tsd_xxx
        value_str = tsd_name[4:]
        if value_str.isdecimal():
            if int(value_str) == member_:
                ret_val = True
    return ret_val
# ----------------------------------------------------------
# Get the ID of the cluster used by this node
# ----------------------------------------------------------
def get_node_cluster_id():
    global cluster_id_
    return cluster_id_

# ----------------------------------------------------------
# Get metadata version - return the load counter
# ----------------------------------------------------------
def get_metadata_version():
    global load_counter_
    return load_counter_

# ----------------------------------------------------------
# Test the metadata version
# ----------------------------------------------------------
def test_metadata_version(version):
    global load_counter_
    if not load_counter_:
        return False            # No version available
    return load_counter_ == version


# ----------------------------------------------------------
# Reset the metadata structures
# ----------------------------------------------------------
def reset_structure():
    global table_to_cluster_
    global cluster_to_servers_
    global active_flag_

    table_to_cluster_ = {}  # Mapping from dbms + table to the clusters supporting the distributions
    cluster_to_servers_ = {}  # Mapping from cluster to the individual servers
    active_flag_ = False

# ----------------------------------------------------------
# Load the requested metadata
# Returns the list of clusters that were declared
# ----------------------------------------------------------
def load(status, clusters):

    global load_counter_
    global cluster_to_servers_
    global dbms_to_company_

    ret_val = process_status.SUCCESS
    cluster_list = []

    for cluster_info in clusters:
        message = test_cluster(status, cluster_info)
        if message != "ok":
            status.add_error(message)
            continue  # A problem with the cluster definition

        cluster = cluster_info["cluster"]

        if "parent" in cluster:
            # This is a declaration of a table created after the initial cluster was defined
            cluster_id = cluster["parent"]
        else:
            cluster_id = cluster["id"]

        if "company" in cluster:
            company = cluster["company"]
        else:
            company = "none"  # Company name if not specified

        cluster_name = cluster["name"]

        if "status" in cluster:
            cluster_stat = cluster["status"]
        else:
            cluster_stat = "active"

        if cluster_stat == "active" or cluster_stat == "static":

            if "table" in cluster:          # With table definitions
                tables_list = cluster["table"]

                for table_info in tables_list:
                    # Get definitions for the specific table
                    table_name = table_info["name"]
                    dbms_name = table_info["dbms"]

                    dbms_to_company_[dbms_name] = company       # Determines (for publishers) = companies by databases

                    distribution = get_number_of_distributions(company, dbms_name, table_name) + 1  # Get the number of existing distributions + 1

                    # Update the Table to Cluster structure: Company -> DBMS -> (Table + status) -> (Distribution + Cluster ID)
                    ret_val = update_metadata(status, cluster_id, cluster_name, cluster_stat, company, dbms_name, table_name, distribution)
                    if ret_val:
                        break

            # b) Update Cluster to Operators
            # This is a cluster policy without tables defined in the policy
            ret_val = register_cluster(status, cluster_id, cluster_stat, cluster_name, company)

            if ret_val:
                break

            if not cluster_id in cluster_list:
                cluster_list.append(cluster_id)

    load_counter_ += 1   # Counter for every time that load called

    return [ret_val, cluster_list]  # Return the list of clusters that were used

# ----------------------------------------------------------
# Return the number of distributions in the metadata to a given table
# ----------------------------------------------------------
def get_number_of_distributions(company, dbms_name, table_name):

    try:
        distributions = len(table_to_cluster_[company][dbms_name][table_name]["distribution"])  # length of the list of distributions (if exists)
    except:
        distributions = 0
    return distributions

# ----------------------------------------------------------
# Update: a) TABLE to CLUSTER b) CLUSTER to OPERATORS 
# The Table to Cluster structure:
# Company -> DBMS -> (Table + status) -> (Distribution + Cluster ID)
# ----------------------------------------------------------
def update_metadata(status, cluster_id, cluster_name, cluster_stat, company, dbms_name, table_name, distr):

    global table_to_cluster_
    global cluster_to_servers_

    ret_val = process_status.SUCCESS
    # a) TABLE to CLUSTER

    if company not in table_to_cluster_:
        table_to_cluster_[company] = {}
    company_dict = table_to_cluster_[company]
    if dbms_name not in company_dict:
        company_dict[dbms_name] = {}
    dbms_dict = company_dict[dbms_name]
    if table_name not in dbms_dict:
        table_info = {}
        distribution = []
        table_info["distribution"] = distribution  # List the different clusters associated with this table
        table_info["status"] = "active"         # The table is active by default
        table_info["round_robin"] = 0          # An index to the next cluster to user
        dbms_dict[table_name] = table_info
    else:
        table_info = dbms_dict[table_name]
        distribution = table_info["distribution"]

        # Test no duplicate defenition of the same cluster in the same table
        if cluster_id in distribution:
            err_msg = "The cluster: '%s' is associated with the same table multiple times: company: '%s' table: %s.%s" % (cluster_id, company, dbms_name, table_name)
            status.add_error(err_msg)
            utils_print.output_box(err_msg)
            ret_val = process_status.Wrong_policy_structure

    if not ret_val:

        distribution.append(cluster_id)

    return ret_val

# ----------------------------------------------------------
# Update the cluster_to servers list
# ----------------------------------------------------------
def register_cluster(status, cluster_id, cluster_stat, cluster_name, company):

    ret_val = process_status.SUCCESS
    if cluster_id not in cluster_to_servers_:
        cluster = {
            "name": cluster_name,
            "company": company
        }
        cluster_to_servers_[cluster_id] = cluster
    else:
        cluster = cluster_to_servers_[cluster_id]
        if cluster["name"] != cluster_name:
            status.add_error("Cluster: '%s' is associated with multiple names: '%s' and '%s'" % (
            cluster_id, cluster["name"], cluster_name))
            ret_val = process_status.Wrong_policy_structure
        elif cluster["company"] != company:
            status.add_error("Cluster: '%s' is associated with multiple companies: '%s' and '%s'" % (
            cluster_id, cluster["company"], company))
            ret_val = process_status.Wrong_policy_structure

    if not ret_val:
        cluster["status"] = cluster_stat
        if "operators" not in cluster_to_servers_[cluster_id]:
            cluster["operators"] = []
            cluster["round_robin"] = [0,0]      # 2 ids of the operator to use - The first is for query and the second for data distribution

    return ret_val

# ----------------------------------------------------------
# When an operator process starts - it provides the Operator policy ID
# associated with this node
# ----------------------------------------------------------
def set_operator_policy(policy_id):
    global operator_policy_id_

    operator_policy_id_ = policy_id
# ----------------------------------------------------------
# Update Operators that support the clusters
# ip_port are the TCP used by this server to communicate - it allows to determine the current_member id
# ----------------------------------------------------------
def update_operators(status, operators, trace):
    global cluster_to_servers_
    global ip_port_to_operator_      # A dictionary thap mapps an IP + port to the cluster info
    global member_
    global cluster_id_
    global active_flag_
    global company_name_
    global cluster_name_
    global operator_name_
    global operator_policy_id_
    global update_time_


    member_ = 0
    cluster_id_ = "0"
    update_time_ = get_current_time_in_sec()

    for operator_status in ip_port_to_operator_.values():
        # Remove the link to the operator info as this will change with the new load
        # But we keep the status entry - as a new metadata setup should not change the node state
        if "operator" in operator_status:
            del operator_status["operator"]

    ret_val = process_status.SUCCESS
    for operator in operators:
        operator_info = operator["operator"]
        if "id" not in operator_info:
            continue
        operator_id = operator_info["id"]
        if "cluster" in operator_info and "member" in operator_info:

            if "ip" not in operator_info or "port" not in operator_info:
                message = "Missing IP or port in declaration of Operator with id: %s" % operator_id
                status.add_error(message)
                utils_print.output_box(message)
                reset_structure()
                ret_val = process_status.Wrong_policy_structure
                break

            cluster_id = operator_info["cluster"]
            operator_ip_port = operator_info["ip"] + ":" + str(operator_info["port"])    # Need to be the external ip


            if operator_id == operator_policy_id_:
                # This is the current node
                # This info is used by the Operator process even if not all cluster info is available (i.e. no table definitions yet)
                if cluster_id_ != '0':
                    message = "Multiple Operators with the same IP and Port: '%s'" % operator_ip_port
                    status.add_error(message)
                    utils_print.output_box(message)
                    reset_structure()
                    ret_val = process_status.Wrong_policy_structure
                    break

                member_ = operator_info["member"]  # Declared globally
                if "name" in operator_info:
                    operator_name_ = operator_info["name"]

                cluster_id_ = cluster_id
                if cluster_id in cluster_to_servers_:
                    company_name_ = cluster_to_servers_[cluster_id]["company"]
                    cluster_name_ = cluster_to_servers_[cluster_id]["name"]

            if cluster_id in cluster_to_servers_:

                # Test Operator policy has all needed info
                 # Test no multiple operators with the same IP and Port

                if operator_ip_port in ip_port_to_operator_:
                    ip_port_info = ip_port_to_operator_[operator_ip_port]
                    if "operator" in ip_port_info:
                        # the same ip_port can show only a single object
                        message = "Duplicate assignments of Operators with the same IP and Port: %s" % operator_ip_port
                        status.add_error(message)
                        utils_print.output_box(message)
                        reset_structure()
                        ret_val = process_status.Wrong_policy_structure
                        break
                else:
                    # For every IP+ Port, show a link to the Operator info and the status of the operator
                    ip_port_to_operator_[operator_ip_port] = {}
                    ip_port_to_operator_[operator_ip_port]["status"] = "active"
                    ip_port_to_operator_[operator_ip_port]["status_time"] = get_current_utc_time()
                    if "type" in operator_info and operator_info["type"].lower() == "data source":
                        ip_port_to_operator_[operator_ip_port]["foreign_net"] = True  # This server is from a foreign network - used to provide data source
                    else:
                        ip_port_to_operator_[operator_ip_port]["foreign_net"] = False  # This server is from a foreign network - used to provide data source

                ip_port_to_operator_[operator_ip_port]["operator"] = operator_info      # Map IP + port to the operator info in the cluster

                operators_list = cluster_to_servers_[cluster_id]["operators"]
                updated = False
                for index, operator in enumerate(operators_list):
                    if operator_id == operator["id"]:
                        # Operator is in the structure -> update operator
                        updated = True
                        break
                if not updated:
                    # New Operator
                    operators_list.append(operator_info)
        if ret_val:
            break

    if trace:
        utils_print.output(f"\r\n[This node policies] [Operator Policy: {operator_policy_id_}] [Cluster Policy: {cluster_id_}]", True)
    if not ret_val:
        active_flag_ = True     # All was set successfully
        if trace:
            utils_print.output(f"\r\n[Local Metadata enabled]", True)
    else:
        if trace:
            utils_print.output(f"\r\n[Local Metadata NOT enabled] [Error Message:{process_status.get_status_text(ret_val)}]" , True)
        active_flag_ = False
    return ret_val

# ----------------------------------------------------------
# Set the status of the operator to be: "active" or not responding
# ----------------------------------------------------------
def set_operator_status(status, ip, port, mode, foreign_net):


    if ip == net_utils.get_local_ip():
        # Use the flag on the external IP
        external_ip = net_utils.get_external_ip()
    else:
        external_ip = ip

    port_str =str(port)
    ip_port = external_ip + ":" + port_str

    if ip_port in ip_port_to_operator_:     # Avoid the mutex if not needed
        operator_info = ip_port_to_operator_[ip_port]
        if operator_info["status"] != mode:
            read_lock()

            if ip_port in ip_port_to_operator_:
                operator_info = ip_port_to_operator_[ip_port]
                if mode:
                    operator_info["status"] = mode
                if foreign_net:
                    operator_info["foreign_net"] = foreign_net  # True - the server is from a foreign network - used to provide data source (to extract files)
                operator_info["status_time"] = get_current_utc_time()

            read_unlock()

# ----------------------------------------------------------
# Get the status of the operator that failed
# ----------------------------------------------------------
def get_peers_status(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3:
        ret_val = process_status.SUCCESS
        peer_stat = []
        for ip_port, peer_info in  ip_port_to_operator_.items():
            peer_stat.append((ip_port, peer_info["status"], peer_info["status_time"]))

        info_peers = utils_print.output_nested_lists(peer_stat, "", ["Address", "Status", "Recorded time"], True, "      ")
    else:
        ret_val = process_status.ERR_command_struct
        info_peers = ""

    return [ret_val, info_peers]
# ----------------------------------------------------------
# Validate the structure of the cluster info, or return an errror message
# ----------------------------------------------------------
def test_clusters_list(status, clusters):
    info = "ok"
    for cluster_info in clusters:
        info = test_cluster(status, cluster_info)
        if info != "ok":
            break
    return info
# ----------------------------------------------------------
# Validate the structure of the cluster info, or return an errror message
# ----------------------------------------------------------
def test_cluster(status, cluster_info):
    if not "cluster" in cluster_info:
        return "Policy provided is not a 'cluster' policy"

    cluster = cluster_info["cluster"]

    if not "id" in cluster:
        return "Missing ID declaration in 'cluster' policy"  # Need to have table declaration with the list of tables supported by the cluster

    cluster_id = cluster["id"]

    if not "name" in cluster:
        return "Missing name in 'cluster' policy '%s'" % cluster_id

    if "status" in cluster:
        cluster_stat = cluster["status"]
        if cluster_stat != "active" and cluster_stat != "static" and cluster_stat != "down":
            return "Cluster %s: Wrong status in 'cluster' policy - key: [cluster][status]" % cluster_id

    if "table" in cluster:
        tables_list = cluster["table"]
        if not isinstance(tables_list, list):
            return "Cluster %s: Table declaration in 'cluster' policy is not a list - key: [cluster][table]" % cluster_id

        for table_info in tables_list:
            # Get definitions for the specific table
            if not "name" in table_info:
                return "Cluster %s: Missing table name in 'cluster' policy - key: [cluster][table][name]" % cluster_id
            if not "dbms" in table_info:
                return "Cluster %s: Missing dbms name in 'cluster' policy - key: [cluster][table][dbms]" % cluster_id

            if "status" in table_info:
                table_stat = table_info["status"]
                if table_stat != "active" and table_stat != "static" and table_stat != "down":
                    return "Cluster %s: Wrong status in 'cluster' policy - key: [cluster][table][status]" % cluster_id

    return "ok"
# ----------------------------------------------------------
# Query distributions with conditions
# ----------------------------------------------------------
def query(status, conditions, is_print):

    global table_to_cluster_
    global cluster_to_servers_

    query_info = ""

    company_name = interpreter.get_one_value_or_default(conditions, "company", None)
    dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
    table_name = interpreter.get_one_value_or_default(conditions, "table", None)


    if company_name:
        # Query only one company
        query_info += query_company(status, company_name, dbms_name, table_name, conditions, is_print)
    else:
        # Query all companies
        for company in table_to_cluster_:                 # Loop on companies
            utils_print.output("\r\n", False)
            query_info += query_company(status, company, dbms_name, table_name, conditions, is_print)


    query_info += utils_print.dynamic_tab("\r\n", False, 0, is_print)
    return query_info
# ----------------------------------------------------------
# Query distributions with conditions
# ----------------------------------------------------------
def query_company(status, company, dbms_name, table_name, conditions, is_print):

    query_info = utils_print.dynamic_tab(company, True, 0, is_print)

    dbms_offset = len(company)  # Offset to dbms info

    company_info = table_to_cluster_[company]

    if dbms_name:
        # Only one DBMS
        query_info += query_dbms(status, company_info, dbms_name, table_name, 0, dbms_offset, len(dbms_name), conditions, is_print)
    else:
        # Set offset according to the dbms names length
        max_dbms_name_len = 0
        for dbms in company_info:
            if len(dbms) > max_dbms_name_len:
                max_dbms_name_len = len(dbms)

        for dbms_counter, dbms in enumerate(company_info):  # Loop on databases
            query_info += query_dbms(status, company_info, dbms, table_name, dbms_counter, dbms_offset, max_dbms_name_len, conditions, is_print)

    return query_info
# ----------------------------------------------------------
# Query distributions with conditions
# ----------------------------------------------------------
def query_dbms(status, company_info, dbms, table_name, dbms_counter, dbms_offset, max_dbms_name_len, conditions, is_print):

    new_line, offset_tab = query_format(dbms_counter, dbms_offset)

    dbms_str = dbms.ljust(max_dbms_name_len)
    query_info = utils_print.dynamic_tab(" ==> %s" % dbms_str, new_line, offset_tab, is_print)

    table_offset = dbms_offset + 5 + max_dbms_name_len

    dbms_info = company_info[dbms]

    if table_name:
        query_info += query_table(status, dbms_info, table_name, 0, table_offset, len(table_name), conditions, is_print)
    else:
        # Set offset according to the table names length
        max_table_name_len = 0
        for table in dbms_info:
            if len(table) > max_table_name_len:
                max_table_name_len = len(table)

        # get every table info
        for table_counter, table in enumerate(dbms_info):  # Loop on tables
            query_info += query_table(status, dbms_info, table, table_counter, table_offset, max_table_name_len, conditions, is_print)

    return query_info

# ----------------------------------------------------------
# Query distributions with conditions
# ----------------------------------------------------------
def query_table(status, dbms_info, table, table_counter, table_offset, max_table_name_len, conditions, is_print):

    global ip_port_to_operator_

    new_line, offset_tab = query_format(table_counter, table_offset)

    table_str = table.ljust(max_table_name_len)
    query_info = utils_print.dynamic_tab(" ==> %s" % table_str, new_line, offset_tab, is_print)

    distr_offset = table_offset + 5 + max_table_name_len

    table_info = dbms_info[table]
    distribution = table_info["distribution"]

    # Get the max length of the distribution name
    cluster_name_length = 0
    for cluster_id in distribution:
        if cluster_id in cluster_to_servers_:
            if "name" in cluster_to_servers_[cluster_id]:
                print_length = len(cluster_id) + len(cluster_to_servers_[cluster_id]["name"]) + 3    # The + 3 are for the space between the strings and parenthesis
                if  print_length > cluster_name_length:
                    cluster_name_length = print_length

    for index, cluster_id in enumerate(distribution):  # Loop on the clusters per each table

        new_line, offset_tab = query_format(index, distr_offset)

        if cluster_id not in cluster_to_servers_:
            cluster_string = (" ==> %s" % cluster_id).ljust(cluster_name_length)
            query_info += utils_print.dynamic_tab(cluster_string, new_line, offset_tab, is_print)
        else:
            if "name" in cluster_to_servers_[cluster_id]:
                cluster_string = "%s (%s)" % (cluster_id, cluster_to_servers_[cluster_id]["name"])
            else:
                cluster_string = cluster_id
            cluster_string = cluster_string.ljust(cluster_name_length)

            query_info += utils_print.dynamic_tab(" ==> %s" % cluster_string, new_line, offset_tab, is_print)

            operators_list = cluster_to_servers_[cluster_id]["operators"]
            opr_offset = distr_offset + 5 + cluster_name_length

            for opr_counter, operator in enumerate(operators_list):

                # Get the external ip
                if "ip" in operator:
                    ip = operator["ip"]
                else:
                    ip = "unknown"

                if "port" in operator:
                    port = str(operator["port"])
                else:
                    port = "unknown"

                # Get the local ip
                if "local_ip" in operator:
                    local_ip_port = operator["local_ip"] + ":"
                    if "local_port" in operator:
                        local_ip_port += str(operator["local_port"])
                    else:
                        local_ip_port += port
                else:
                    local_ip_port = ""

                member_id = operator["member"]
                if not member_id:
                    member_id = -1                          # Indicates missing member_id
                elif not isinstance(member_id, int):
                    try:
                        member_id = int(member_id)
                    except:
                        member_id = -1  # Indicates Error in member_id

                if member_id == member_:
                    node_info = "%04u  local  " % member_id
                else:
                    node_info = "%04u  remote " % member_id

                ip_port = ip + ':' + port
                if ip_port in ip_port_to_operator_:
                    node_info += ip_port_to_operator_[ip_port]["status"]
                else:
                    node_info += "active"

                node_info = ("[%s]" % node_info).ljust((30))

                ip_port = ip_port.ljust(25)

                new_line, offset_tab = query_format(opr_counter, opr_offset)
                query_info += utils_print.dynamic_tab(" ==> %s      %s %s" % (node_info, ip_port, local_ip_port), new_line, offset_tab, is_print)

    return query_info
# ----------------------------------------------------------
# Determine new line and Tab for the query output of the metadata
# ----------------------------------------------------------
def query_format(counter, offset):
    if not counter:
        offset_tab = 0
        new_line = False
    else:
        offset_tab = offset
        new_line = True
    return [new_line, offset_tab]

# ----------------------------------------------------------
# Get All Operators by DBMS and Table
# ----------------------------------------------------------
def get_all_operators(status, dest_keys, include_tables):

    if len(dest_keys) >= 2:
        dbms_name = dest_keys[0][5:]        # after "dbms="
        table_name = dest_keys[1][6:]        # after "table="
    else:
        return []       # Returns empty list without table name and dbms name

    return get_all_operators_by_keys(status, dbms_name, table_name, False)

# ----------------------------------------------------------
# Get Operators by DBMS and Table
# rr_id is the round robin ID:
#   0 - used by the query process
#   1 - used by Operator for distribution process
# ----------------------------------------------------------
def get_operators(status, dest_keys, include_tables, rr_id, is_dict):
    '''
    dest_keys is a list with the followinf format:
                    "dbms=[dbms name]"
                    "table=[table name]
    include_tables = the tables that determine the operators
    rr_id is the round robin ID
    if is_dict is True - a dictionary is returned, otherwise an object of type operator_info
    '''

    if len(dest_keys) >= 2:
        dbms_name = dest_keys[0][5:]        # after "dbms="
        table_name = dest_keys[1][6:]        # after "table="
        ret_val, operators_list = get_operators_by_keys(status, dbms_name, table_name, rr_id, is_dict)
        if include_tables:
            for inc_table in include_tables:
                index = inc_table.find('.')
                if index > 1 and index < (len(inc_table) - 1):
                    # Change the database name as inc_tables appears as dbms.table
                    db_name = inc_table[:index]
                    tb_name = inc_table[index+1:]
                else:
                    db_name = dbms_name
                    tb_name = inc_table

                ret_val, inc_operators = get_operators_by_keys(status, db_name, tb_name, rr_id, is_dict)
                if ret_val:
                    break
                operators_list += inc_operators     # Add the operators of the included tables

    else:
        operators_list = []     # Returns empty list without table name and dbms name
        ret_val = process_status.Missing_dest_dbms_table

    return [ret_val, operators_list]
# ----------------------------------------------------------
# Get Operators by DBMS and Table - 1 operator per distribution
# rr_id is the round robin ID:
#   0 - used by the query process
#   1 - used by Operator for distribution process
# ----------------------------------------------------------
def get_operators_by_keys(status, dbms_name, table_name, rr_id,  is_dict):
    '''
    if is_dict is True - a dictionary is returned, otherwise an object of type operator_info
    '''

    operators_list = []

    ret_val = process_status.Operator_not_available

    # search for operators that satisfy the conditions
    for company in table_to_cluster_.keys():                 # Loop on companies

        company_info = table_to_cluster_[company]
        if dbms_name in company_info:
            dbms_info = company_info[dbms_name]     # dbms name found
            if table_name in dbms_info:
                table_info = dbms_info[table_name]
                distribution = table_info["distribution"]      # The list of clusters

                for cluster_id in distribution:
                    if cluster_id in cluster_to_servers_:
                        cluster = cluster_to_servers_[cluster_id]
                        ret_val, operator = get_any_operator(status, cluster, rr_id)
                        active_operator = True
                        if not operator:
                            if status.is_subset():
                                # Does not require reply from all clusters - user query is:
                                # run client (subset = true, timeout = 5) sql ...
                                try:
                                    operator =  cluster["operators"][0] # Take first operator
                                except:
                                    break
                                else:
                                    ret_val = process_status.SUCCESS     # Allowed because subset is set to True
                                    active_operator = False # Flag that the operator is not active
                            else:
                                break   # Try a different company with the same dbms name and table name
                        if "ip" not in operator or "port" not in operator:
                            status.add_error("Metadata: Missing Operator IP or Port for Company: '%s' and DBMS: '%s' and table: '%s' and Cluster: '%s'" % (company, dbms_name, table_name, cluster_id))
                            ret_val = process_status.Missing_operator_ip_port
                            break
                        operator_info = get_info_from_operator(operator, dbms_name, table_name, is_dict, active_operator)
                        operators_list.append(operator_info)
        if ret_val and ret_val != process_status.Operator_not_available:
            # If Operator_not_available, cintinue the loop
            break
    if ret_val:
        status.add_error(f"Failed to determine an Operator Node for table: '{dbms_name}.{table_name}'")
    elif len(operators_list) == 0:
        status.add_error(f"No active operators for table: '{dbms_name}.{table_name}'")
        ret_val = process_status.Missing_operators_for_table

    return [ret_val, operators_list]   # return one operator for each distribution

# ----------------------------------------------------------
# Get All Operators by DBMS and Table
# (returns all operators on each relevant distribution)
# ----------------------------------------------------------
def get_all_operators_by_keys(status, dbms_name, table_name, is_dict):
    '''
    if is_dict is True - a dictionary is returned, otherwise an object of type operator_info
    '''

    operators_list = []

    # search for operators that satisfy the conditions
    for company in table_to_cluster_.keys():                 # Loop on companies

        company_info = table_to_cluster_[company]
        if dbms_name in company_info:
            dbms_info = company_info[dbms_name]     # dbms name found
            if table_name in dbms_info:
                table_info = dbms_info[table_name]
                distribution = table_info["distribution"]      # The list of clusters

                for cluster_id in distribution:
                    if cluster_id in cluster_to_servers_:
                        cluster = cluster_to_servers_[cluster_id]

                        if cluster["status"] == "active":
                            operators_in_cluster = cluster["operators"]

                            for operator in operators_in_cluster:
                                operator_info = get_info_from_operator(operator, dbms_name, table_name, is_dict, True)
                                operators_list.append(operator_info)

    return operators_list   # return one operator for each distribution

# ----------------------------------------------------------
# Get Operator info to a pre-structured object or dictionary
# ----------------------------------------------------------
def get_info_from_operator(operator, dbms_name, table_name, is_dict, is_active):

    ip, port = net_utils.get_dest_ip_port(operator)

    if is_dict:
        # Return a dictionary
        operator_info = {}
        operator_info["ip"] = ip
        operator_info["port"] = port
        operator_info["dbms"] = dbms_name
        operator_info["table"] = table_name
        if "name" in operator:
            operator_info["name"] = operator["name"]
    else:
        # Return an OperatorInfo object
        operator_info = blockchain.OperatorInfo()
        operator_info.set_info(ip, str(port), dbms_name, table_name, is_active)

    return operator_info        # Returns an obj or a dictionary

# ----------------------------------------------------------
# Use Round-Robin to get the next operator from the cluster
# If an Operator is not responsive, do the following:
# Get a next operator which is not flagged as "not_responding"
# rr_id is the round robin ID:
#   0 - used by the query process
#   1 - used by Operator for distribution process
# ----------------------------------------------------------
def get_any_operator(status, cluster, rr_id):

    ret_val = process_status.SUCCESS
    returned_operator = None
    if cluster["status"] == "active":
        operators_list = cluster["operators"]
        counter_operators = len(operators_list)
        for _ in range(counter_operators):
            # Get any operator without flag not_responding
            id = cluster["round_robin"][rr_id]         # The operator to use
            if id >= (counter_operators - 1):
                cluster["round_robin"][rr_id] = 0      # set the next operator to use
            else:
                cluster["round_robin"][rr_id] += 1
            operator = operators_list[id]

            ip_port = operator["ip"] + ":" + str(operator["port"])  # Need to be the external IP

            if ip_port in ip_port_to_operator_:     # ip_port_to_operator_ is an independent structure that is not deleted when metadata is refreshed
                if ip_port_to_operator_[ip_port]["status"] == "active":
                    if not ip_port_to_operator_[ip_port]["foreign_net"]:    # This server is from a foreign network - used to provide data source
                        returned_operator = operator
                        break
    if not returned_operator:
        err_msg = "No active operator for cluster"
        if "name" in cluster:
            err_msg += f": '{cluster['name']}'"
        status.add_error(err_msg)
        ret_val = process_status.Operator_not_available

    return [ret_val, returned_operator]               # return a random operator for this distribution

# ----------------------------------------------------------
# Get the list of operators by a cluster ID
# ----------------------------------------------------------
def get_cluster_operators(status, cluster_id):

    read_lock()
    if cluster_id in cluster_to_servers_:
        cluster = cluster_to_servers_[cluster_id]
        operators = cluster["operators"]
    else:
        operators = None
    read_unlock()
    return operators
# ----------------------------------------------------------
# Use Round-Robin to get the next cluster and in the cluster get
# the next operator
# ----------------------------------------------------------
def get_data_destination_operator_ip_port(status, compamy_name, dbms_name, table_name, rr_id):

    ip_port = ""
    operator = get_operator_json(status, compamy_name, dbms_name, table_name, rr_id)

    if operator:
        ip, port = net_utils.get_dest_ip_port(operator)
        if ip and port:
            ip_port = ip + ':' + str(port)

    return ip_port
# ----------------------------------------------------------
# Get an operator that satisfies the condition (company, dbms_name, table_name)
# ----------------------------------------------------------
def get_operator_json(status, compamy_name, dbms_name, table_name, rr_id):

    global cluster_to_servers_

    if not compamy_name:
        company = "none"  # Company name if not specified
    else:
        company = compamy_name

    read_lock()

    table_info = get_table_info(status, company, dbms_name, table_name)

    if table_info:

        distribution = table_info["distribution"]  # List of clusters
        next_cluster = table_info["round_robin"]  # The cluster to use
        cluster_id = distribution[next_cluster]
        next_cluster += 1  # set on next cluster to use
        if next_cluster >= len(distribution):
            next_cluster = 0
        table_info["round_robin"] = next_cluster  # set next cluster to use

        cluster = cluster_to_servers_[cluster_id]
        ret_val, operator = get_any_operator(status, cluster, rr_id)
    else:
        operator = None

    read_unlock()

    return operator
# ----------------------------------------------------------
# Gt the list of distributions for a table
# ----------------------------------------------------------
def get_table_info(status, compamy_name, dbms_name, table_name):

    global table_to_cluster_

    table_info = None

    if compamy_name in table_to_cluster_:
        company_info = table_to_cluster_[compamy_name]
        if dbms_name in company_info:
            dbms_info = company_info[dbms_name]
            if table_name in dbms_info:
                table_info = dbms_info[table_name]

    return table_info

# =======================================================================================================================
# Lock metadata - write
# Called from member_cmd blockchain_load
# =======================================================================================================================
def write_lock():
    metadata_mutex.acquire_write()
# =======================================================================================================================
# UnLock metadata - write
# =======================================================================================================================
def write_unlock():
    metadata_mutex.release_write()
# =======================================================================================================================
# Lock metadata  - read
# =======================================================================================================================
def read_lock():
    metadata_mutex.acquire_read()
# =======================================================================================================================
# UnLock metadata  - read
# =======================================================================================================================
def read_unlock():
    metadata_mutex.release_read()
# =======================================================================================================================
# Test if the member ID is a valid member of the cluster
# =======================================================================================================================
def test_cluster_member(cluster_id,  member_id):
    ret_val = False
    if cluster_id in cluster_to_servers_:
        cluster = cluster_to_servers_[cluster_id]
        if "operators" in cluster_to_servers_[cluster_id]:
            operators = cluster["operators"]
            for operator in operators:
                if operator["member"] == member_id:
                    ret_val = True
                    break
    return ret_val

# =======================================================================================================================
# Get info from the list of operators - return a list where every entry describes an operator
# If cluster ID in None - get the cluster supported by this node
# with_current determines if to include this node
# =======================================================================================================================
def get_operators_info(status:process_status, cluster_id, with_current:bool, attr_names:list):
    global cluster_id_

    operators_list = []

    read_lock()

    if not cluster_id:
        cluster_id = cluster_id_

    if cluster_to_servers_ and cluster_id in cluster_to_servers_:
        info_size = len(attr_names)             # The number of elements on each operator
        cluster = cluster_to_servers_[cluster_id]
        operators = cluster["operators"]
        for entry in operators:
            if not with_current:
                # ignore this node info
                if member_ == entry["member"]:
                    continue

            if "ip" in attr_names and "port" in attr_names:
                # if IP and Port are needed, replace to local IP and Port if Operator is on the local network
                dest_ip, dest_port = net_utils.get_dest_ip_port(entry)
            else:
                dest_ip = None
                dest_port = None

            # add the needed attributes
            operator_info = ()
            for attr in attr_names:
                if not attr in entry:
                    if attr == "operator_status":
                        # take the value from the ip_port_to_operator_ structure
                        ip_port = entry["ip"] + ":" + str(entry["port"])        # Need to be the external ip
                        if ip_port in ip_port_to_operator_:
                            value = ip_port_to_operator_[ip_port]["status"]
                        else:
                            break
                    else:
                        break
                else:
                    if attr == "ip" and dest_ip:
                        value = dest_ip         # The destination IP which can be a local IP if this server and the destination server are on the same network
                    elif attr == "port" and dest_port:
                        value = dest_port
                    else:
                        value = entry[attr]
                operator_info += (value,)
            if len(operator_info) == info_size:
                # All attributes are available
                operators_list.append(operator_info)

    read_unlock()

    return operators_list


# =======================================================================================================================
# Test if the given table is supported by this clister
# =======================================================================================================================
def is_table_in_cluster(status, company, dbms_name, table_name, cluster_id ):

    global cluster_id_          # The policy id of the operator
    global table_to_cluster_

    ret_val = False

    # This node is declared as an operator
    clusters = None
    read_lock()
    if company:
        if company in table_to_cluster_:
            if dbms_name in table_to_cluster_[company] and table_name in table_to_cluster_[company][dbms_name]:
                clusters = table_to_cluster_[company][dbms_name][table_name]
    else:
        for dbms_info in table_to_cluster_.values():
            # Go over all companies to find dbms and table
            if dbms_name in dbms_info and table_name in dbms_info[dbms_name]:
                clusters = dbms_info[dbms_name][table_name]
                break

    if clusters:
        ret_val = cluster_id in clusters["distribution"]

    read_unlock()

    return ret_val

# =======================================================================================================================
# Test if the given table is supported by this node operator
# =======================================================================================================================
def is_table_supported(status, company, dbms_name, table_name ):

    global cluster_id_          # The policy id of the operator
    global table_to_cluster_

    ret_val = False

    if cluster_id_ != '0':
        # This node is declared as an operator
        clusters = None
        read_lock()
        if company:
            if company in table_to_cluster_:
                if dbms_name in table_to_cluster_[company] and table_name in table_to_cluster_[company][dbms_name]:
                    clusters = table_to_cluster_[company][dbms_name][table_name]
        else:
            for dbms_info in table_to_cluster_.values():
                # Go over all companies to find dbms and table
                if dbms_name in dbms_info and table_name in dbms_info[dbms_name]:
                    clusters = dbms_info[dbms_name][table_name]
                    break

        if clusters:
            ret_val = cluster_id_ in clusters["distribution"]

        read_unlock()
    return ret_val

# =======================================================================================================================
# Show info in a reply to a show command - show cluster info
# =======================================================================================================================
def show_info(status):
    global table_to_cluster_
    global cluster_name_
    global company_name_

    if not is_active():
        info_str = "Cluster data is not available"
    else:
        info_str =  "\r\nCluster ID  : %s" % cluster_id_
        info_str += "\r\nMember ID   : %s" % member_
        if cluster_name_:
            info_str += "\r\nCluster Name: %s" % cluster_name_
        if company_name_:
            info_str += "\r\nCluster Name: %s" % company_name_

        info_str += "\r\nParticipating Operators:"
        operators_list = get_operators_info(status, cluster_id_, True, ["ip", "port", "member", "operator_status"])
        info_str += utils_print.output_nested_lists(operators_list, "", ["IP", "Port","Member", "Status" ], True, "      ")

        info_str += "\r\nTables Supported:"

        tables_list = []

        read_lock()

        for company_name, company_info in table_to_cluster_.items():  # Loop on companies
            for dbms_name, dbms_info in company_info.items():
                for table_name, table_info in dbms_info.items():
                    distribution = table_info["distribution"]
                    if cluster_id_ in distribution:
                        # This table is in this cluster
                        tables_list.append((company_name, dbms_name, table_name))
        read_unlock()

        info_str += utils_print.output_nested_lists(tables_list, "", ["Company", "DBMS","Table" ], True, "      ")

    return info_str


# =======================================================================================================================
# Update the member status based on messages with the state of the TSD table
# The update is the result of a message: events.get_recent_tsd_info
#
# validated_id - The ID that represents safe data - all the prior ids ON THIS NODE exists.
#                This ID is updated once by a message from the PEER Node, and after the first update, by THIS NODE to represent validated data
# src_first_id - The ID of the start date on the PEER NODE. THIS NODE requets the PEER NODE to provide the IDs representing a start date and an end date.
#                The src_first_id is not used, THIS NODE is operating to get the validated_id as close as possible to the src_last_id.
# src_last_id - The last ID of the TSD table in the PEER NODE - The most updated Row on the PEER NODE
# =======================================================================================================================
def update_member_status(table_name, first_date, first_id, last_date, last_id):
    global member_status_

    if not table_name in member_status_:
        # First update - set the values for the safe data.
        # After the first update, the node determines which IDs are missing to bring the files and update the value of
        # the validated_id.
        mem_stat = {}
        member_status_[table_name] = mem_stat
        mem_stat["validated_date"] = first_date       # Earlier data is tested to exists
        mem_stat["validated_id"] = int(first_id)      # All IDS up to end_id exists on the current node
    else:
        mem_stat = member_status_[table_name]
        mem_stat["src_first_date"] = first_date           # The date-time nect to the FROM "date time" requested (on the PEER NODE)
        mem_stat["src_first_id"] = int(first_id)          # The ID representing the FROM "date time" requested (on the PEER NODE)

    current_time = get_current_utc_time()
    mem_stat["message_time"] = current_time        # The time the message was received
    mem_stat["src_last_date"] = last_date
    mem_stat["src_last_id"] = int(last_id)

# =======================================================================================================================
# Return the safe TSD ID for every TSD table which is managed on this node
# =======================================================================================================================
def get_safe_tsd_ids():
    global member_status_

    reply_str = "*"
    for table_name, mem_stat in member_status_.items():

        if "validated_date" in mem_stat:
            reply_str += (table_name + '~' + str(mem_stat["validated_id"]) + '*')

    return reply_str






# =======================================================================================================================
# Get member status - this table is processed from the alconsumer.get_missing_files
# =======================================================================================================================
def get_member_stat(table_name):
    global member_status_
    if table_name in member_status_:
        mem_stat = member_status_[table_name]
    else:
        mem_stat = None
    return mem_stat
# =======================================================================================================================
# Show the status of one or all TSD tables
# =======================================================================================================================
def show_tsd_status(tsd_table):
    global member_status_

    reply_str = ""
    if tsd_table == "all":
        for table_name, entry in  member_status_.items():
            reply_str += get_tsd_table_status(table_name, entry)
    else:
        # Only one table
        if tsd_table in member_status_:
            entry = member_status_[tsd_table]
            reply_str = get_tsd_table_status(tsd_table, entry)

    if not reply_str:
        reply_str = "\r\nInfo on TSD not available"
    return reply_str
# =======================================================================================================================
# Get the safe ID for each TSD table
# The Safe ID is the validated ID
# =======================================================================================================================
def get_safe_ids(tsd_table):
    global member_
    global member_status_

    safe_ids = {}

    if tsd_table == "all":
        for table_name, entry in  member_status_.items():
            validated = entry["validated_id"]        # The Safe ID on this node
            safe_ids[table_name] = {
                str(member_) : validated,
                "consensus" : validated               # This value is the common (lowest) Safe ID among all nodes of the cluster
            }
    else:
        # Only one table
        if tsd_table in member_status_:
            entry = member_status_[tsd_table]
            validated = entry["validated_id"]  # The Safe ID on this node
            safe_ids[tsd_table] = {
                str(member_): validated,
                "consensus": validated  # This value is the common (lowest) Safe ID among all nodes of the cluster
            }


    return safe_ids

# =======================================================================================================================
# Return the status of one TSD tables
# =======================================================================================================================
def get_tsd_table_status(table_name, tsd_info):

    # Change UTC to current time
    tsd_current = {}

    for key, value in tsd_info.items():
        if key[-5:] == "_time" or  key[-5:] == "_date":
            # Change utc to current
            print_value = utc_to_timezone(value, '%Y-%m-%d %H:%M:%S', "local")
        else:
            print_value = value
        tsd_current[key] = print_value      # update the new value

    reply_str = utils_print.format_dictionary(tsd_current, True, False, False, ["Event: %s" % table_name, "Value"])
    return reply_str

# =======================================================================================================================
# Send ping to all the non-responsive servers
# =======================================================================================================================
def get_failed_servers():

    servers_list = []       # A list of the non-responsive servers

    read_lock()

    for ip_port, operator in ip_port_to_operator_.items():  # ip_port_to_operator_ is an independent structure that is not deleted when metadata is refreshed
        if operator["status"] != "active":
            servers_list.append(ip_port)

    read_unlock()

    return servers_list

# =======================================================================================================================
# Determine the company name by the dbms name
# =======================================================================================================================
def get_company_by_dbms(dbms_name):

    global dbms_to_company_

    if dbms_name in  dbms_to_company_:
        company_name = dbms_to_company_[dbms_name]
    else:
        company_name = None

    return company_name

# =======================================================================================================================
# Get servers by table
# =======================================================================================================================
def get_operators_by_company(company, dbms, table):
    global table_to_cluster_
    target_list = []
    if company == '*':
        for company_name, dbms_dict in table_to_cluster_.items():
            add_dbms_info(target_list, company_name, dbms, table, dbms_dict)
    else:
        if company in table_to_cluster_:
            dbms_dict = table_to_cluster_[company]
            add_dbms_info(target_list, company, dbms, table, dbms_dict)
    return target_list
# =======================================================================================================================
# Add info from the database dictionary forward
# =======================================================================================================================
def add_dbms_info(target_list, company_name, dbms, table, dbms_dict):

    if dbms == '*':     # get all databases from the compamy
        for dbms_name, table_dict in dbms_dict.items():
            add_table_info(target_list, company_name, dbms_name, table, table_dict)
    else:
        if dbms in dbms_dict:
            table_dict = dbms_dict[dbms]
            add_table_info(target_list, company_name, dbms, table, table_dict)
# =======================================================================================================================
# Add info from the table dictionary forward
# =======================================================================================================================
def add_table_info(target_list, company_name, dbms_name, table, table_dict):

    if table == '*':     # get all tables from the dbms
        for table_name, cluster_dict in table_dict.items():
            add_cluster_info(target_list, company_name, dbms_name, table_name, cluster_dict)
    else:
        if table in table_dict:
            cluster_dict = table_dict[table]
            add_cluster_info(target_list, company_name, dbms_name, table, cluster_dict)
# =======================================================================================================================
# Add info from the cluster dictionary forward
# =======================================================================================================================
def add_cluster_info(target_list, company_name, dbms_name, table_name, cluster_dict):

    cluster_state = cluster_dict["status"]
    distribution = cluster_dict["distribution"]
    company = company_name
    dbms = dbms_name
    table = table_name

    for cluster_id in distribution:
        if cluster_id in cluster_to_servers_:
            operators_list = cluster_to_servers_[cluster_id]["operators"]
            for operator in operators_list:
                if "name" in operator:
                    name = operator["name"]
                else:
                    name = ""
                if "member" in operator:
                    member = operator["member"]
                else:
                    member = ""
                if "ip" in operator:
                    ip = operator["ip"]
                else:
                    ip = ""
                if "port" in operator:
                    port = str(operator["port"])
                else:
                    port = ""

                ip_port = ip + ':' + port if (ip or port) else ""


                if "local_ip" in operator:
                    local_ip = operator["local_ip"]
                else:
                    local_ip = ""
                if "local_port" in operator:
                    local_port = str(operator["local_port"])
                else:
                    if local_ip:
                        local_port = port
                    else:
                        local_port = ""

                local_ip_port = local_ip + ':' + local_port if (local_ip or local_port) else ""

                if ip_port in ip_port_to_operator_:
                    node_state = ip_port_to_operator_[ip_port]["status"]
                else:
                    node_state = "active"


                target_list.append((company, dbms, table, cluster_id, cluster_state, name, member,\
                                    ip_port, local_ip_port, node_state))

                cluster_id = company = dbms = table = ""


# =======================================================================================================================
# Get the list of companies from the table_to_cluster_ structure
# company -> dbms -> table -> distribution ID + cluster supporting the distribution ID
# =======================================================================================================================
def get_companies_list():
    global table_to_cluster_
    return list(table_to_cluster_.keys())
# =======================================================================================================================
# Get the list of databases from the table_to_cluster_ structure
# company -> dbms -> table -> distribution ID + cluster supporting the distribution ID
# =======================================================================================================================
def get_dbms_list( company ):
    global table_to_cluster_
    if company in table_to_cluster_:
        dbms_list = list(table_to_cluster_[company].keys())
    else:
        dbms_list = None
    return dbms_list
# =======================================================================================================================
# Get the list of tables from the table_to_cluster_ structure
# company -> dbms -> table -> distribution ID + cluster supporting the distribution ID
# =======================================================================================================================
def get_tables_list( company, dbms ):
    global table_to_cluster_
    if company in table_to_cluster_ and dbms in table_to_cluster_[company]:
        tables_list = list(table_to_cluster_[company][dbms].keys())
    else:
        tables_list = None
    return tables_list

