"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# --------------------------------------------------------------------------------------------------
#  The Gateway is an abstract class allows to interact with a blockchain network.
#  It provides a simple API to submit transactions to a ledger or query the contents of a ledger.
# --------------------------------------------------------------------------------------------------

from abc import ABC, abstractmethod

import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.process_status as process_status

class BlockchainNode(ABC):
    def __init__(self, provider):
        '''
        provider - the URL to the provider node
        '''

        self.provider = provider

        self.private_key = None
        self.public_key = None
        self.private_key = None
        self.contract = None

        self.gas_read = 0            # Payment for a read process
        self.gas_write = 0          # payment for a write process

        self.connection = None
        self.senderAccount = None
        self.connected = False

        self.nonce_values = {}      # maintain the last nonce used as a value of the public key as a transaction is set with a unique nonce.

        self.certificate_dir = ""    # For hyperledger this would be the location of the hyperledger certificate files
        self.config_file = ""       # For hyperledger this would be the "network.json" file with the network connection info.
        self.contract_name = ""


    def get_certificate_dir(self):
        return self.certificate_dir
    def get_config_file(self):
        return self.config_file
    # -------------------------------------------------------------
    # Set the private key and derive the account
    # -------------------------------------------------------------
    def set_private_key(self, private_key):
        self.private_key = private_key  # "a4caa21209188ef5c3be6ee4f73c12a8c306a917c969638fb69f164b0ed95380"

    # -------------------------------------------------------------
    # Set the contract with the connection
    # -------------------------------------------------------------
    def set_contract(self, contract):
        self.contract = contract    # "0x3899bED34d9e3032fb0d544CB76bA7F752Bf5EbE"

    def set_public_key(self, public_key):
        self.public_key = public_key        # "0x982AF5e1589f1486b4bA17aFB6eb940aAeBBdfdB"
    def set_gas_read(self, gas_read):
        self.gas_read = gas_read
    def set_gas_write(self, gas_write):
        self.gas_write = gas_write
    # -------------------------------------------------------------
    # Return the connection URL
    # -------------------------------------------------------------
    def get_provider(self):
        return self.provider
    # -------------------------------------------------------------
    # Return the public key
    # -------------------------------------------------------------
    def get_public_key(self):
        return self.public_key
    # -------------------------------------------------------------
    # Return the contract address
    # -------------------------------------------------------------
    def get_contract_addr(self):
        return self.contract
    # -------------------------------------------------------------
    # Return the contract name (if available, i.e. Hyperledger)
    # -------------------------------------------------------------
    def get_contract_name(self):
        return self.contract_name

   # ------------------------------------------
    # Set parameters for the platform
    # ------------------------------------------
    def set_account_info(self, status, conditions):
        '''
        conditions - a dictionary with key value pairs derived from the user command "blockchain set account info where platform = ethereum ..."
                     The pairs include the user input that allows to connect to a wallet with tokens to pay for the blockchain processes (as needed).
                     These value include - the public and private keys and the contract to use.
        '''

        ret_val = process_status.SUCCESS

        platform_name = interpreter.get_one_value(conditions, "platform")

        # Ethereum params
        if platform_name == "ethereum":
            private_key = interpreter.get_one_value(conditions, "private_key")
            public_key = interpreter.get_one_value(conditions, "public_key")
            contract = interpreter.get_one_value(conditions, "contract")
            gas_read = interpreter.get_one_value_or_default(conditions, "gas_read", 0)
            gas_write = interpreter.get_one_value_or_default(conditions, "gas_write", 0)


            if private_key:
                self.set_private_key(private_key)
                ret_val = self.set_sender_account(status)   # Creates an account object from a private key.
            if not ret_val and contract:
                self.set_contract(contract)
                ret_val = self.set_contract_address(status)     # Create Contract Address

            if not ret_val:
                if public_key:
                    self.set_public_key(public_key)
                if gas_read:
                    self.set_gas_read(gas_read)
                if gas_write:
                    self.set_gas_write(gas_write)

        elif platform_name == "hyperledger":
            # Hyperledger params
            self.certificate_dir = interpreter.get_one_value_or_default(conditions, "certificate_dir",
                                                                        "")  # For hyperledger this would be the location of the hyperledger certificate files
            self.config_file = interpreter.get_one_value_or_default(conditions, "config_file",
                                                                    "")  # For hyperledger this would be the "network.json" file with the network connection info.

        return ret_val

    # -------------------------------------------------------------
    # Connect to platform
    # -------------------------------------------------------------
    @abstractmethod
    def make_connection(self, status, conditions):
        pass

    # -------------------------------------------------------------
    # Create sender Account
    # -------------------------------------------------------------
    @abstractmethod
    def set_sender_account(self, status):
        pass

    # -------------------------------------------------------------
    # Create Contract Address
    # -------------------------------------------------------------
    @abstractmethod
    def set_contract_address(self, status):
        pass

    # -------------------------------------------------------------
    # Create an account
    # -------------------------------------------------------------
    @abstractmethod
    def create_account(self, status):
        pass

    # -------------------------------------------------------------
    # Return the account balance
    # For new Ether - https://faucet.rinkeby.io/
    # -------------------------------------------------------------
    @abstractmethod
    def get_balance(self):
        pass

    # -------------------------------------------------------------
    # Test connection
    # 2 Conditions needs to be satisfied:
    #   a) If connection flag is set (connection was established)
    #   b) Is not disconnected - calling the Ethereum API to validate connection
    # -------------------------------------------------------------
    @abstractmethod
    def is_connected(self):
        pass

    # -------------------------------------------------------------
    # Return the contract instance
    # -------------------------------------------------------------
    @abstractmethod
    def get_contract_instance(self):
        pass

    # -------------------------------------------------------------
    # Add data to the blockchain - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    @abstractmethod
    def put_policies(self, status, policy_id, policy):
        pass

    # -------------------------------------------------------------
    # Delete Policy - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    @abstractmethod
    def delete_policies(self, status, policy_id):
        pass
    # -------------------------------------------------------------
    # Retrieve the data from the blockchain - either data or keys
    # Extracts the pertinent logs from a transaction receipt.
    # If there are no errors, processReceipt returns a tuple of Event Log Objects, emitted from the event (e.g. myEvent), with decoded ouput.
    # -------------------------------------------------------------
    @abstractmethod
    def get_policies(self, status, is_data):
        pass
    # -------------------------------------------------------------
    # Deploy contract
    # Create a transaction, sign it locally, and then send it to your node for broadcasting, with send_raw_transaction().
    # https://support.blockdaemon.com/hc/en-us/articles/360022161812-How-To-Deploy-A-Smart-Contract-with-An-Ethereum-Node-Using-Web3py
    # -------------------------------------------------------------
    @abstractmethod
    def deploy_contract(self, status, public_key):
        pass
   # -------------------------------------------------------------
   # Get transaction count - returns 0 if not implemented
   # Returns Number of transactions for the given address
   # https://web3py.readthedocs.io/en/stable/web3.eth.html
   # -------------------------------------------------------------
    @abstractmethod
    def get_trn_count(self, status, address):
        return [process_status.NOT_SUPPORTED, -1]
