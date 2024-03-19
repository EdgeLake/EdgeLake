pragma experimental ABIEncoderV2;
pragma solidity>=0.6.0;

contract AnyLog {
    struct info {
        string data;
        bool exists;
    }
    
    event json_event(string data);
    event keys_event(string[] keys);
    mapping(string => info) jsonData;
    string [] key_array;
    
    function insert(string memory key, string memory data)
    public{
        require(jsonData[key].exists == false, "key already exists");
        jsonData[key] = info(data, true);
        key_array.push(key);
    }
    
    function get(string memory key)
    public payable{
        require(jsonData[key].exists == true, 'Key does not exist');
        emit json_event(jsonData[key].data);
    }
    
    function get_all_keys()
    public payable {
        emit keys_event(key_array);
    }
    
}
