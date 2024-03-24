"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# Library: https://web3py.readthedocs.io/en/stable/
# Tutorial:  https://github.com/cooganb/web3py-tutorial
# Contracts https://web3py.readthedocs.io/en/stable/examples.html?highlight=infura#working-with-contracts

import sys
try:
    from web3 import HTTPProvider, Web3
    from web3.middleware import geth_poa_middleware
except:
    web3_installed = False
else:
    web3_installed = True

import edge_lake.blockchain.gateway as gateway
import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
# import edge_lake.generic.interpreter as interpreter

# --------------------------------------------------------------------------------------
#  Connect to Ethereum - using the provider connection URL,
# Init an AnyLog Object with the Ethereum info and calls
# --------------------------------------------------------------------------------------
def connect(status, provider):
    '''
    provider - the URL to the Ethereum provider

    returns - SUCCESS or Error code + the AnyLog Object
    '''

    if not web3_installed:
        eth_connection = None
        status.add_error("Failed to connect to Ethereum: web3 not installed")
        ret_val = process_status.Failed_to_import_lib
    else:
        try:
            eth_connection = Ethereum(provider)   # Init an ANyLog object
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = f"Failed to connect to Ethereum with provider:{str(provider)} with error: {errno}, {value}"
            status.add_error(err_msg)
            ret_val = process_status.Connection_error
            eth_connection = None
        else:
            ret_val = process_status.SUCCESS

    return [ret_val, eth_connection]


abi_ = [
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": False,
				"internalType": "string[]",
				"name": "data",
				"type": "string[]"
			}
		],
		"name": "get_data_event",
		"type": "event"
	},
	{
		"anonymous": False,
		"inputs": [
			{
				"indexed": False,
				"internalType": "string[]",
				"name": "keys",
				"type": "string[]"
			}
		],
		"name": "get_key_event",
		"type": "event"
	},
	{
		"inputs": [],
		"name": "get",
		"outputs": [],
		"stateMutability": "payable",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "getKeys",
		"outputs": [],
		"stateMutability": "payable",
		"type": "function"
	},
	{
		"inputs": [
			{
				"internalType": "string",
				"name": "key",
				"type": "string"
			},
			{
				"internalType": "string",
				"name": "json",
				"type": "string"
			}
		],
		"name": "insert",
		"outputs": [],
		"stateMutability": "nonpayable",
		"type": "function"
	}
]


byte_code = "608060405234801561001057600080fd5b506106e0806100206000396000f3fe6080604052600436106100345760003560e01c806306e63ff8146100395780632150c518146100625780636d4ce63c1461006c575b600080fd5b34801561004557600080fd5b50610060600480360361005b919081019061031e565b610076565b005b61006a6101b1565b005b6100746101eb565b005b600082604051602001610089919061053c565b6040516020818303038152906040528051906020012090506002600082815260200190815260200160002060000160009054906101000a900460ff1615610105576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016100fc9061055e565b60405180910390fd5b60016002600083815260200190815260200160002060000160006101000a81548160ff02191690831515021790555060008290806001815401808255809150506001900390600052602060002001600090919091909150908051906020019061016f929190610225565b506001839080600181540180825580915050600190039060005260206000200160009091909190915090805190602001906101ab929190610225565b50505050565b7f3551e759561f6f325e4c4188eb0e2a94a01b1d65207a280fe74ba4d1744f630060016040516101e1919061051a565b60405180910390a1565b7f0b3f84bc423ff1ac110242dc231547adffdd9528d299d82c8b9fc4fa4bee62cd600060405161021b919061051a565b60405180910390a1565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061026657805160ff1916838001178555610294565b82800160010185558215610294579182015b82811115610293578251825591602001919060010190610278565b5b5090506102a191906102a5565b5090565b6102c791905b808211156102c35760008160009055506001016102ab565b5090565b90565b600082601f8301126102db57600080fd5b81356102ee6102e9826105ab565b61057e565b9150808252602083016020830185838301111561030a57600080fd5b610315838284610657565b50505092915050565b6000806040838503121561033157600080fd5b600083013567ffffffffffffffff81111561034b57600080fd5b610357858286016102ca565b925050602083013567ffffffffffffffff81111561037457600080fd5b610380858286016102ca565b9150509250929050565b6000610396838361044b565b905092915050565b60006103a982610601565b6103b38185610624565b9350836020820285016103c5856105d7565b8060005b85811015610400578484038952816103e1858261038a565b94506103ec83610617565b925060208a019950506001810190506103c9565b50829750879550505050505092915050565b600061041d8261060c565b6104278185610646565b9350610437818560208601610666565b61044081610699565b840191505092915050565b600081546001811660008114610468576001811461048e576104d2565b607f60028304166104798187610635565b955060ff1983168652602086019350506104d2565b6002820461049c8187610635565b95506104a7856105ec565b60005b828110156104c9578154818901526001820191506020810190506104aa565b80880195505050505b505092915050565b60006104e7601383610646565b91507f4a534f4e20616c726561647920657869737473000000000000000000000000006000830152602082019050919050565b60006020820190508181036000830152610534818461039e565b905092915050565b600060208201905081810360008301526105568184610412565b905092915050565b60006020820190508181036000830152610577816104da565b9050919050565b6000604051905081810181811067ffffffffffffffff821117156105a157600080fd5b8060405250919050565b600067ffffffffffffffff8211156105c257600080fd5b601f19601f8301169050602081019050919050565b60008190508160005260206000209050919050565b60008190508160005260206000209050919050565b600081549050919050565b600081519050919050565b6000600182019050919050565b600082825260208201905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b82818337600083830152505050565b60005b83811015610684578082015181840152602081019050610669565b83811115610693576000848401525b50505050565b6000601f19601f830116905091905056fea2646970667358221220f1b3ba4f45902d94dec6a3b8065abef974865c4fd2aa7211a9b6615444d7955564736f6c63430006000033"

contract_address = ""

class Ethereum(gateway.BlockchainNode):
    '''
    An Object containing the connection info to Ethereum
    '''

    def set_public_key(self, public_key):
        self.public_key = Web3.to_checksum_address((public_key))

    # -------------------------------------------------------------
    # Connect to platform
    # -------------------------------------------------------------
    def make_connection(self, status, conditions):

        if not web3_installed:
            ret_val = process_status.Failed_to_import_lib
        else:
            try:

                self.connection = Web3(HTTPProvider(self.provider))      # 'https://rinkeby.infura.io/v3/45e96d7ac85c4caab102b84e13e795a1'

                self.connection.middleware_onion.inject(geth_poa_middleware, layer=0)

            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Ethereum failed to connect to provider with error: %s" % (str(value))
                status.add_error(err_msg)
                ret_val = process_status.Connection_error
                self.connected = False
            else:

                ret_val = process_status.SUCCESS
                if self.private_key:
                    ret_val = self.set_sender_account(status)
                if not ret_val and self.contract:
                    ret_val = self.set_contract_address(status)
                if not ret_val:
                    self.connected = True

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
            self.senderAccount = self.connection.eth.account.from_key(self.private_key)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Ethereum failed to associate an account with the provided private key - Error: %s" % str(value)
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
            self.contract_address = self.connection.to_checksum_address((self.contract)) # Returns the given address with an EIP55 checksum.
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Ethereum failed to associate a contract ti an account - Error: %s" % str(value)
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
            new_account = self.connection.eth.account.create('AnyLog TestNet Account Create!')
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Ethereum failed to Create Account with error: %s" % (str(value))
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

        try:
            wei = self.connection.eth.get_balance( self.public_key )
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Failed to extract balance: {0} : {1}".format(str(errno), str(value))
            process_log.add("Error", err_msg)
            reply = "Failed to extract balance"
        else:
            ether = wei/1000000000000000000
            wei -= int(ether) * 1000000000000000000
            reply = "Ether: %u Wei: %u" % (int(ether), wei)

        return reply
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
                valid_connect = self.connection.is_connected()
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Ethereum is_connected failed: {0} : {1}".format(str(errno), str(value))
                process_log.add("Error", err_msg)

                valid_connect = False
        else:
            valid_connect = False

        return valid_connect

    # -------------------------------------------------------------
    # Return the contract instance
    # -------------------------------------------------------------
    def get_contract_instance(self):
        global abi_

        ret_val = process_status.SUCCESS
        try:
            contract_address = self.contract_address
        except:
            err_msg = "Ethereum contract not assigned (use: 'blockchain set account info' command)"
            process_log.add("Error", err_msg)
            contract_instance = None
            ret_val = process_status.BLOCKCHAIN_contract_error
        else:
            try:
                contract_instance = self.connection.eth.contract(address=contract_address, abi=abi_)
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Ethereum contract failure: {0} : {1}".format(str(errno), str(value))
                process_log.add("Error", err_msg)
                contract_instance = None
                ret_val = process_status.BLOCKCHAIN_contract_error

        return [ret_val, contract_instance]

    # -------------------------------------------------------------
    # Add data to the blockchain - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    def put_policies(self, status, policy_id, policy):

        if not self.connected:
            status.add_error("Ethereum platform not connected")
            ret_val = process_status.Connection_error
            reply = None
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if ret_val:
                reply = None
            else:
                try:
                    nonce = self.get_next_nonce(self.public_key)


                    transaction = contract_instance.functions.insert(policy_id, policy).build_transaction(
                        {
                            # 'gas': self.gas_write,     # 3172000
                            #'chainId': 5,           # Goerli chain ID - need to externalize
                            'chainId':11155111,        # SEPOLIA
                            # 'gasPrice': self.w3.toWei('1000000', 'wei'),
                            'nonce': nonce,
                        }
                    )

                    signed_tx = self.sign_tx(transaction)

                    tx_hash = self.send_raw_tx(signed_tx)

                    tx_receipt = self.wait_for_tx_receipt(tx_hash)
                    reply = f'RECEIPT {tx_receipt}'
                except:
                    reply = None
                    errno, value = sys.exc_info()[:2]
                    err_msg = "Blockchain Failure: new policy not written: {0} : {1}".format(str(errno), str(value))
                    status.add_error(err_msg)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    ret_val = process_status.SUCCESS

        return [ret_val, reply]

    # -------------------------------------------------------------
    # Delete Policy - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    def delete_policies(self, status, policy_id):
        pass
    # -------------------------------------------------------------
    # Retrieve the data from the blockchain - either data or keys
    # Extracts the pertinent logs from a transaction receipt.
    # If there are no errors, processReceipt returns a tuple of Event Log Objects, emitted from the event (e.g. myEvent), with decoded ouput.
    # -------------------------------------------------------------
    def get_policies(self, status, is_data):

        if not self.connected:
            status.add_error("Ethereum platform not connected")
            ret_val = process_status.Connection_error
            data = None
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if ret_val:
                data = None
            else:
                try:
                    nonce = self.get_next_nonce(self.senderAccount.address)


                    # Builds a transaction dictionary based on the contract function call specified.
                    # Info at https://web3py.readthedocs.io/en/stable/contracts.html?highlight=buildTransaction#web3.contract.ContractFunction.build_transaction
                    # Details at https://web3js.readthedocs.io/en/v1.2.0/web3-eth.html?highlight=transaction#id62
                    transaction = contract_instance.functions.get().build_transaction(
                        {

                            # 'gas': self.gas_read,     # 3000000
                            #'chainId': 5,       # Goerli chain ID - need to externalize
                            'chainId': 11155111,  # SEPOLIA
                            # 'gasPrice': self.w3.toWei('1000000', 'wei'),
                            'nonce': nonce,
                        }
                    )
                    # Transaction includes maxFeePerGas - this is the sum of: baseFeePerGas (determined by the network) + maxPriorityFeePerGas (determined by the miners

                    signed_tx = self.sign_tx(transaction)
                    tx_hash = self.send_raw_tx(signed_tx)
                    tx_receipt = self.wait_for_tx_receipt(tx_hash)

                    if is_data:
                        logs = contract_instance.events.get_data_event().process_receipt(tx_receipt)
                        key = "data"
                    else:
                        logs = contract_instance.events.get_key_event().process_receipt(tx_receipt)
                        key = "keys"
                except:
                    data = None

                    errno, value = sys.exc_info()[:2]
                    err_msg = "Blockchain Failure: retrieve data from blockchain failed: {0} : {1}".format(str(errno), str(value))
                    status.add_error(err_msg)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if len(logs) and "args" in logs[0] and key in logs[0]['args']:
                        data = logs[0]['args'][key]
                    else:
                        data = None

                    ret_val = process_status.SUCCESS

        return [ret_val, data]

    # -------------------------------------------------------------
    # Deploy contract
    # Create a transaction, sign it locally, and then send it to your node for broadcasting, with send_raw_transaction().
    # https://support.blockdaemon.com/hc/en-us/articles/360022161812-How-To-Deploy-A-Smart-Contract-with-An-Ethereum-Node-Using-Web3py
    # -------------------------------------------------------------
    def deploy_contract(self, status, public_key):

        global abi_
        if not self.connected:
            status.add_error("Ethereum platform not connected")
            contract_address = None
            ret_val = process_status.Connection_error
        else:
            try:

                tx_hash = self.connection.eth.contract( abi=abi_, bytecode=byte_code)        # create contract object
                # Builds a transaction dictionary based on the contract function call specified.
                # Info at https://web3py.readthedocs.io/en/stable/contracts.html?highlight=buildTransaction#web3.contract.ContractFunction.build_transaction
                # Details at https://web3js.readthedocs.io/en/v1.2.0/web3-eth.html?highlight=transaction#id62
                transaction = tx_hash.constructor().build_transaction()              # method to prepare contract transaction
                nonce = self.get_next_nonce(public_key)
                transaction['nonce'] =nonce  # Get correct transaction nonce for sender from the node
                signed_tx = self.sign_tx(transaction)
                # Send it!
                txHash = self.connection.eth.send_raw_transaction(signed_tx.rawTransaction)   # sendRawTransaction Deprecated: This method is deprecated in favor of send_raw_transaction()

                # Waits for the transaction specified by transaction_hash to be included in a block, then returns its transaction receipt.
                tx_receipt = self.connection.eth.wait_for_transaction_receipt(txHash)          # waitForTransactionReceipt: Deprecated: This method is deprecated in favor of wait_for_transaction_receipt()
                contract_address = tx_receipt['contractAddress']
                ret_val = process_status.SUCCESS
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = "Ethereum deploy contract failed: {0} : {1}".format(str(errno), str(value))
                status.add_error(err_msg)
                contract_address = None
                ret_val = process_status.BLOCKCHAIN_operation_failed

        return [ret_val, contract_address]

    # -------------------------------------------------------------
    # Get thr number of transactions for a given address
    # -------------------------------------------------------------
    def get_trn_count(self, status, address):

        try:
            counter = self.connection.eth.get_transaction_count(address)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Ethereum transaction count failed: {0} : {1}".format(str(errno), str(value))
            status.add_error(err_msg)
            counter = -1
            ret_val = process_status.BLOCKCHAIN_operation_failed
        else:
            ret_val = process_status.SUCCESS

        return [ret_val, counter]

    def wait_for_tx_receipt(self, tx_hash):
        '''
        Waits for the transaction specified by transaction_hash to be included in a block, then returns its transaction receipt.
        Optionally, specify a timeout in seconds. If timeout elapses before the transaction is added to a block, then wait_for_transaction_receipt() raises a web3.exceptions.TimeExhausted exception.
        waitForTransactionReceipt is deprecated in favor of wait_for_transaction_receipt()
        '''
        tx_receipt = self.connection.eth.wait_for_transaction_receipt(tx_hash)
        return tx_receipt

    def send_raw_tx(self, signed_tx):
        '''
        Sends a signed and serialized transaction. Returns the transaction hash as a HexBytes object.
        sendRawTransaction is deprecated, use -  send_raw_transaction() Details -
        https://web3py.readthedocs.io/en/latest/web3.eth.html?highlight=send_raw_transaction#web3.eth.Eth.send_raw_transaction
        '''
        tx_hash = self.connection.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash

    def sign_tx(self, transaction):
        '''
        Returns a transaction that’s been signed by the node’s private key, but not yet submitted. The signed tx can be submitted with Eth.send_raw_transaction
        signTransaction is deprecated in favor of sign_transaction()
        https://web3py.readthedocs.io/en/latest/web3.eth.html?highlight=signTransaction#web3.eth.Eth.signTransaction
        '''
        signed = self.senderAccount.sign_transaction(transaction)
        return signed


    # -------------------------------------------------------------
    # Get the next nonce
    # The nonce is an increasing numeric value which is used to uniquely identify transactions.
    # A nonce can only be used once and until a transaction is mined,
    # it is possible to send multiple versions of a transaction with the same nonce, however,
    # once mined, any subsequent submissions will be rejected.

    # Per key, we get the first nonce, then we increment the nonce to avoid rejection if previous transaction is not mined.
    # -------------------------------------------------------------
    def get_next_nonce(self, key):
        # Key is the key used to generate the nonce
        # return next nonce to use

        if key in self.nonce_values:
            self.nonce_values[key] += 1     # set on next
            nonce = self.nonce_values[key]
        else:
            nonce = self.connection.eth.get_transaction_count(key)
            self.nonce_values[key] = nonce
        return nonce