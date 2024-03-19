pragma experimental ABIEncoderV2;
pragma solidity>=0.6.0;

contract AnyLog2 {
    
    string[] data;
    
    
    string[] keys;
    
    struct Data {
        bool exists;
    }
    
    mapping(bytes32 => Data) used_data;
    
    event get_data_event(string[] data);
    event get_key_event(string[] key);
    
    
    function insert(string memory key, string memory json)
    public{
        bytes32 hashed_data = keccak256(abi.encode(key));
        require(! used_data[hashed_data].exists, "JSON already exists");
        used_data[hashed_data].exists = true;
        data.push(json);
        keys.push(key);
    }
    
    function get()
    public payable{
        emit get_data_event(data);
    }
    
    function getKeys() 
    public payable{
        emit get_key_event(keys);
    }

    

}