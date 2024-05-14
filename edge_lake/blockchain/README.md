# Blockchain 

## Setting up new Blockchain
1. Download [MetaMask Extension](https://chrome.google.com/webstore/detail/metamask/nkbihfbeogaeaoehlefnkodbefgpgknn#:~:text=MetaMask%20is%20an%20extension%20for%20accessing%20Ethereum%20enabled,so%20that%20dapps%20can%20read%20from%20the%20blockchain.) 

2. Create or import account
   * **Account Paraphrase**: `cloud expect salad party penalty hen card notable receive develop rule advance` 
   * **Privae Key**: `0x982AF5e1589f1486b4bA17aFB6eb940aAeBBdfdB`

3. Switch Rinkeby Test Network

4.  In a new tab open [Etherium Remix](https://remix.ethereum.org/) & switch to classic view

5. Update [code](alethereum/AnyLog2.sol) to: 
```
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
```
6. Set compile version 0.6.0-nightly.2019.12.12+commit.104a8c59.Emscripten.clang

7. Set "autocompile" 

8. Switch to "Run" tab

9. Press "Deploy" 

## Starting AnyLog Instance 
1. The bottom of the screen should generate a URL that looks something like this: 
```
https://rinkeby.etherscan.io/tx/0x53698b56526de9708af3bc6f7cced4d4b6054bbb5aedba2540db3e4b0a080c85
```

2. Open the link & copy the Contract address (imaged shown) 
![screen shot](Screen_Shot.png) 

3. In  AnyLog run the the following: 
```
# Update the Contract based on the value in the generated link 
<blockchain connect ethereum where
        provider = "https://rinkeby.infura.io/v3/45e96d7ac85c4caab102b84e13e795a1" and
        contract = "0xa4607b299fce6df1d0eb8ea06765c808b8f25791" and
        private_key = "a4caa21209188ef5c3be6ee4f73c12a8c306a917c969638fb69f164b0ed95380" and 
        public_key = "0x982AF5e1589f1486b4bA17aFB6eb940aAeBBdfdB" and 
        gas_read = 2000000  and
        gas_write = 3000000>
```

## Adding Funds
1. Go to [Rinkeby Faucet](https://rinkeby.faucet.epirus.io/#) 

2. Set you Address 

3. Press "Send Ether" 

This should add 0.2ETH and can be done only once every 24 hours.  
