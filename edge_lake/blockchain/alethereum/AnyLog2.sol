// SPDX-License-Identifier: MPL-2.0
pragma experimental ABIEncoderV2;
pragma solidity>=0.6.0;

contract AnyLog2 {

    struct Policy {
        string data;
        uint policies_index;
        address policy_owner;
        bool exists;
    }

    string[] public policies;

    string[] public policy_ids;
    uint public num_policies = 0;

    uint public transaction_count = 0;

    mapping(string => Policy) policy_store;

    event get_policy_event(string);
    event delete_policy_event(bool);
    event get_policy_owner_event(address);
    event updated_policy_event(string, string);


    function insert(string calldata policy_id, string calldata json)
    external {
        require(!policy_store[policy_id].exists, "Policy with policy id already exists");
        policies.push(json);
        policy_ids.push(policy_id);
        policy_store[policy_id] = Policy(json, policies.length-1, msg.sender, true);
        transaction_count += 1;
        num_policies += 1;
    }

    function getPolicy(string calldata policy_id) external  {
        require(policy_store[policy_id].exists, "Policy does not exist");
        uint policy_index = policy_store[policy_id].policies_index;
        emit get_policy_event(policies[policy_index]);
    }

    function deletePolicy(string calldata policy_id) external  {
        require(policy_store[policy_id].exists, "Policy ID does not exist");
        transaction_count += 1;
        policy_store[policy_id].exists = false;
        uint policy_index = policy_store[policy_id].policies_index;

        uint last_policy_index = policies.length-1;

        policies[policy_index] = policies[last_policy_index];
        string memory p_id = policy_ids[last_policy_index];
        policy_store[p_id].policies_index = policy_index;
        policy_ids[policy_index] = p_id;

        policies.pop(); // delete last index of policies array
        policy_ids.pop(); // delete last index of complementary policy_ids array
        emit delete_policy_event(true);
        num_policies -= 1;
    }

    function updatePolicy(string calldata policy_id, string calldata json) external {
        require(policy_store[policy_id].exists, "Policy ID does not exist");
        require(policy_store[policy_id].policy_owner == address(msg.sender), "Only policy owner can update policy");
        policy_store[policy_id] = Policy(json, policy_store[policy_id].policies_index, msg.sender, true); // update policies map
        policies[policy_store[policy_id].policies_index] = json; // update policies list
        transaction_count += 1;
        emit updated_policy_event(policy_id, json);
    }

    function updatePolicy(string calldata policy_id, string calldata json) external {
        require(policy_store[policy_id].exists, "Policy ID does not exist");
        require(policy_store[policy_id].policy_owner == address(msg.sender), "Only policy owner can update policy");
        policy_store[policy_id] = Policy(json, policy_store[policy_id].policies_index, msg.sender, true); // update policies map
        policies[policy_store[policy_id].policies_index] = json; // update policies list
        transaction_count += 1;
        emit updated_policy_event(policy_id, json);
    }

    function getPolicyOwner(string calldata policy_id) external {
        emit get_policy_owner_event(policy_store[policy_id].policy_owner);
    }

}