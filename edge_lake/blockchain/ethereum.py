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
    import web3.logs
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
            eth_connection = Ethereum(provider)  # Init an ANyLog object
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = f"Failed to connect to Ethereum with provider:{str(provider)} with error: {errno}, {value}"
            status.add_error(err_msg)
            ret_val = process_status.Connection_error
            eth_connection = None
        else:
            ret_val = process_status.SUCCESS

    return [ret_val, eth_connection]


abi_ = [{"anonymous":False,"inputs":[{"indexed":False,"internalType":"bool","name":"","type":"bool"}],"name":"delete_policy_event","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"string[]","name":"","type":"string[]"}],"name":"get_all_policies_event","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"string[]","name":"","type":"string[]"}],"name":"get_all_policy_ids_event","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"string","name":"","type":"string"}],"name":"get_policy_event","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"","type":"address"}],"name":"get_policy_owner_event","type":"event"},{"inputs":[{"internalType":"string","name":"policy_id","type":"string"}],"name":"deletePolicy","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getAllPolicies","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getAllPolicyIds","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"policy_id","type":"string"}],"name":"getPolicy","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"policy_id","type":"string"}],"name":"getPolicyOwner","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"policy_id","type":"string"},{"internalType":"string","name":"json","type":"string"}],"name":"insert","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"transaction_count","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]

byte_code = "60806040526000600260005090905534801561001b5760006000fd5b50610021565b61125d806100306000396000f3fe60806040523480156100115760006000fd5b50600436106100825760003560e01c806360dd5f901161005c57806360dd5f90146100b85780636c450e09146100d4578063a0e60c18146100f0578063f7ff50e21461010c57610082565b806306e63ff814610088578063457e5521146100a45780634b254924146100ae57610082565b60006000fd5b6100a2600480360361009d9190810190610c39565b61012a565b005b6100ac610390565b005b6100b6610409565b005b6100d260048036036100cd9190810190610bf0565b610447565b005b6100ee60048036036100e99190810190610bf0565b61054d565b005b61010a60048036036101059190810190610bf0565b6105d2565b005b610114610909565b60405161012191906110e5565b60405180910390f35b6003600050848460405161013f929190610fd2565b908152602001604051809103902060005060020160149054906101000a900460ff161515156101a3576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161019a906110c4565b60405180910390fd5b6000600050828290918060018154018082558091505060019003906000526020600020900160005b9091929091929091929091925091906101e5929190610912565b506001600050848490918060018154018082558091505060019003906000526020600020900160005b909192909192909192909192509190610228929190610912565b50604051806080016040528083838080601f016020809104026020016040519081016040528093929190818152602001838380828437600081840152601f19601f82011690508083019250505050505050815260200160016000600050805490500381526020013373ffffffffffffffffffffffffffffffffffffffff16815260200160011515815260200150600360005085856040516102ca929190610fd2565b908152602001604051809103902060005060008201518160000160005090805190602001906102fa929190610997565b5060208201518160010160005090905560408201518160020160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555060608201518160020160146101000a81548160ff0219169083151502179055509050506001600260008282825054019250508190909055505b50505050565b7f461d80627c49f4753a01d3483f64d15543e7c352228322e696fe43354d394ad360006000506040516103c39190611020565b60405180910390a17f2e7ce3229a7b2679f2ed621852adb9751e7b9c3103c699f1083de65c40309e8860016000506040516103fe9190611020565b60405180910390a15b565b7f2e7ce3229a7b2679f2ed621852adb9751e7b9c3103c699f1083de65c40309e88600160005060405161043c9190611020565b60405180910390a15b565b6003600050828260405161045c929190610fd2565b908152602001604051809103902060005060020160149054906101000a900460ff1615156104bf576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016104b6906110a3565b60405180910390fd5b6000600360005083836040516104d6929190610fd2565b90815260200160405180910390206000506001016000505490507f17b88547e856ab10e485425d4da97aa7af183cb4c2ba8cd590edb964a812af1660006000508281548110151561052357fe5b906000526020600020900160005b5060405161053f919061105f565b60405180910390a1505b5050565b7f21f82dfd7e71adb54ffba84028263297f5ae30c611b9c52d19e68b408c49ae7f60036000508383604051610583929190610fd2565b908152602001604051809103902060005060020160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff166040516105c59190611004565b60405180910390a15b5050565b600360005082826040516105e7929190610fd2565b908152602001604051809103902060005060020160149054906101000a900460ff16151561064a576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161064190611082565b60405180910390fd5b600160026000828282505401925050819090905550600060036000508383604051610676929190610fd2565b908152602001604051809103902060005060020160146101000a81548160ff0219169083151502179055506000600360005083836040516106b8929190610fd2565b9081526020016040518091039020600050600101600050549050600060016000600050805490500390506000600050818154811015156106f457fe5b906000526020600020900160005b5060006000508381548110151561071557fe5b906000526020600020900160005b509080546001816001161561010002031660029004610743929190610a1c565b50606060016000508281548110151561075857fe5b906000526020600020900160005b508054600181600116156101000203166002900480601f0160208091040260200160405190810160405280929190818152602001828054600181600116156101000203166002900480156107fb5780601f106107d0576101008083540402835291602001916107fb565b820191906000526020600020905b8154815290600101906020018083116107de57829003601f168201915b50505050509050826003600050826040516108169190610fec565b90815260200160405180910390206000506001016000508190909055508060016000508481548110151561084657fe5b906000526020600020900160005b509080519060200190610868929190610aa3565b506000600050805480151561087957fe5b600190038181906000526020600020900160005b6108979190610b28565b9055600160005080548015156108a957fe5b600190038181906000526020600020900160005b6108c79190610b28565b90557f5939cc59f60051e79cb8108ce0404b75eab25be7ebff470256f6861b24cff9e060016040516108f99190611043565b60405180910390a15050505b5050565b60026000505481565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061095357803560ff1916838001178555610986565b82800160010185558215610986579182015b828111156109855782358260005090905591602001919060010190610965565b5b5090506109939190610b70565b5090565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f106109d857805160ff1916838001178555610a0b565b82800160010185558215610a0b579182015b82811115610a0a57825182600050909055916020019190600101906109ea565b5b509050610a189190610b70565b5090565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f10610a555780548555610a92565b82800160010185558215610a9257600052602060002091601f016020900482015b82811115610a91578254825591600101919060010190610a76565b5b509050610a9f9190610b70565b5090565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f10610ae457805160ff1916838001178555610b17565b82800160010185558215610b17579182015b82811115610b165782518260005090905591602001919060010190610af6565b5b509050610b249190610b70565b5090565b50805460018160011615610100020316600290046000825580601f10610b4e5750610b6d565b601f016020900490600052602060002090810190610b6c9190610b70565b5b50565b610b989190610b7a565b80821115610b945760008181506000905550600101610b7a565b5090565b9056611226565b6000600083601f8401121515610bb55760006000fd5b8235905067ffffffffffffffff811115610bcf5760006000fd5b602083019150836001820283011115610be85760006000fd5b5b9250929050565b6000600060208385031215610c055760006000fd5b600083013567ffffffffffffffff811115610c205760006000fd5b610c2c85828601610b9f565b92509250505b9250929050565b600060006000600060408587031215610c525760006000fd5b600085013567ffffffffffffffff811115610c6d5760006000fd5b610c7987828801610b9f565b9450945050602085013567ffffffffffffffff811115610c995760006000fd5b610ca587828801610b9f565b92509250505b92959194509250565b6000610cc08383610db7565b90505b92915050565b610cd281611195565b82525b5050565b6000610ce48261112d565b610cee8185611153565b935083602082028501610d0085611101565b8060005b85811015610d3c57848403895281610d1c8582610cb4565b9450610d2783611145565b925060208a019950505b600181019050610d04565b5082975087955050505050505b92915050565b610d58816111a8565b82525b5050565b6000610d6b8385611189565b9350610d788385846111e1565b82840190505b9392505050565b6000610d9082611139565b610d9a8185611189565b9350610daa8185602086016111f1565b8084019150505b92915050565b600081546001811660008114610dd45760018114610dfa57610e3f565b607f6002830416610de58187611165565b955060ff198316865260208601935050610e3f565b60028204610e088187611165565b9550610e1385611117565b60005b82811015610e36578154818901526001820191505b602081019050610e16565b80880195505050505b50505b92915050565b600081546001811660008114610e655760018114610e8b57610ed0565b607f6002830416610e768187611177565b955060ff198316865260208601935050610ed0565b60028204610e998187611177565b9550610ea485611117565b60005b82811015610ec7578154818901526001820191505b602081019050610ea7565b80880195505050505b50505b92915050565b6000610ee6601883611177565b91507f506f6c69637920494420646f6573206e6f74206578697374000000000000000060008301526020820190505b919050565b6000610f27601583611177565b91507f506f6c69637920646f6573206e6f74206578697374000000000000000000000060008301526020820190505b919050565b6000610f68602483611177565b91507f506f6c696379207769746820706f6c69637920696420616c726561647920657860008301527f697374730000000000000000000000000000000000000000000000000000000060208301526040820190505b919050565b610fcb816111d6565b82525b5050565b6000610fdf828486610d5f565b91508190505b9392505050565b6000610ff88284610d85565b91508190505b92915050565b60006020820190506110196000830184610cc9565b5b92915050565b6000602082019050818103600083015261103a8184610cd9565b90505b92915050565b60006020820190506110586000830184610d4f565b5b92915050565b600060208201905081810360008301526110798184610e48565b90505b92915050565b6000602082019050818103600083015261109b81610ed9565b90505b919050565b600060208201905081810360008301526110bc81610f1a565b90505b919050565b600060208201905081810360008301526110dd81610f5b565b90505b919050565b60006020820190506110fa6000830184610fc2565b5b92915050565b600081905081600052602060002090505b919050565b600081905081600052602060002090505b919050565b6000815490505b919050565b6000815190505b919050565b60006001820190505b919050565b60008282526020820190505b92915050565b60008282526020820190505b92915050565b60008282526020820190505b92915050565b60008190505b92915050565b60006111a0826111b5565b90505b919050565b600081151590505b919050565b600073ffffffffffffffffffffffffffffffffffffffff821690505b919050565b60008190505b919050565b828183376000838301525b505050565b60005b838110156112105780820151818401525b6020810190506111f4565b8381111561121f576000848401525b505b505050565bfea26469706673582212201a58dae7ded99c99596bbfd7a5e5318cab0749e920db193032508c3fa1be0c8764736f6c63430006010033"

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

                self.connection = Web3(
                    HTTPProvider(self.provider))  # 'https://rinkeby.infura.io/v3/45e96d7ac85c4caab102b84e13e795a1'

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
            self.contract_address = self.connection.to_checksum_address(
                (self.contract))  # Returns the given address with an EIP55 checksum.
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
            ethereum_keys = "\r\nPublic key:  %s\r\nPrivate Key: %s" % (
                new_account._address, new_account._private_key.hex())
            ret_val = process_status.SUCCESS

        return [ret_val, ethereum_keys]

    # -------------------------------------------------------------
    # Return the account balance
    # For new Ether - https://faucet.rinkeby.io/
    # -------------------------------------------------------------
    def get_balance(self):

        try:
            wei = self.connection.eth.get_balance(self.public_key)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Failed to extract balance: {0} : {1}".format(str(errno), str(value))
            process_log.add("Error", err_msg)
            reply = "Failed to extract balance"
        else:
            ether = wei / 1000000000000000000
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
                    chain_id = self.get_chain_id()

                    transaction = contract_instance.functions.insert(policy_id, policy).build_transaction(
                        {
                            # 'gas': self.gas_write,     # 3172000
                            'chainId': chain_id,  # ,        # Goerli : 5  SEPOLIA : 11155111
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

        reply = None
        if not self.connected:
            status.add_error("Ethereum platform not connected")
            ret_val = process_status.Connection_error
        else:

            ret_val, contract_instance = self.get_contract_instance()
            if not ret_val:
                try:
                    nonce = self.get_next_nonce(self.public_key)
                    chain_id = self.get_chain_id()

                    # Assuming there's a function called deleteData in the smart contract
                    transaction = contract_instance.functions.deletePolicy(policy_id).build_transaction(
                        {
                            # 'gas': self.gas_write,     # You may need to specify gas limit
                            'chainId': chain_id,
                            # 'gasPrice': self.w3.toWei('1000000', 'wei'),
                            'nonce': nonce,
                        }
                    )

                    signed_tx = self.sign_tx(transaction)

                    tx_hash = self.send_raw_tx(signed_tx)

                    tx_receipt = self.wait_for_tx_receipt(tx_hash)
                    reply = f'RECEIPT {tx_receipt}'
                except:
                    errno, value = sys.exc_info()[:2]
                    err_msg = "Blockchain Failure: retrieve data from blockchain failed: {0} : {1}".format(str(errno),
                                                                                                           str(value))
                    status.add_error(err_msg)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    ret_val = process_status.SUCCESS

        return [ret_val, reply]

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
                    chain_id = self.get_chain_id()

                    # Builds a transaction dictionary based on the contract function call specified.
                    # Info at https://web3py.readthedocs.io/en/stable/contracts.html?highlight=buildTransaction#web3.contract.ContractFunction.build_transaction
                    # Details at https://web3js.readthedocs.io/en/v1.2.0/web3-eth.html?highlight=transaction#id62
                    transaction = contract_instance.functions.getAllPolicies().build_transaction(
                        {

                            # 'gas': self.gas_read,     # 3000000
                            'chainId': chain_id,  # ,        # Goerli : 5  SEPOLIA : 11155111
                            # 'gasPrice': self.w3.toWei('1000000', 'wei'),
                            'nonce': nonce,
                        }
                    )
                    # Transaction includes maxFeePerGas - this is the sum of: baseFeePerGas (determined by the network) + maxPriorityFeePerGas (determined by the miners

                    signed_tx = self.sign_tx(transaction)
                    tx_hash = self.send_raw_tx(signed_tx).hex()
                    tx_receipt = self.wait_for_tx_receipt(tx_hash)

                    # For the next calls we use  errors=DISCARD:
                    # We determined that the MismatchedABI warnings are harmless and it is OK to suppress them by passing errors=DISCARD to the processReceipt method like so:
                    # ContractEvent().processReceipt(receipt, errors=DISCARD)
                    if is_data:
                        # Only rerieve the JSON policies
                        log_policies = contract_instance.events.get_all_policies_event().process_receipt(tx_receipt, errors = web3.logs.DISCARD)
                    else:
                        # only retrieve the IDS
                        log_policies = contract_instance.events.get_all_policy_ids_event().process_receipt(tx_receipt, errors = web3.logs.DISCARD)

                except:
                    data = None

                    errno, value = sys.exc_info()[:2]
                    err_msg = "Blockchain Failure: retrieve data from blockchain failed: {0} : {1}".format(str(errno),
                                                                                                           str(value))
                    status.add_error(err_msg)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if len(log_policies) and "args" in log_policies[0] and '' in log_policies[0]["args"]:
                        data = log_policies[0]["args"]['']
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

                tx_hash = self.connection.eth.contract(abi=abi_, bytecode=byte_code)  # create contract object
                # Builds a transaction dictionary based on the contract function call specified.
                # Info at https://web3py.readthedocs.io/en/stable/contracts.html?highlight=buildTransaction#web3.contract.ContractFunction.build_transaction
                # Details at https://web3js.readthedocs.io/en/v1.2.0/web3-eth.html?highlight=transaction#id62
                transaction = tx_hash.constructor().build_transaction()  # method to prepare contract transaction
                nonce = self.get_next_nonce(public_key)
                transaction['nonce'] = nonce  # Get correct transaction nonce for sender from the node
                signed_tx = self.sign_tx(transaction)
                # Send it!
                txHash = self.connection.eth.send_raw_transaction(
                    signed_tx.rawTransaction)  # sendRawTransaction Deprecated: This method is deprecated in favor of send_raw_transaction()

                # Waits for the transaction specified by transaction_hash to be included in a block, then returns its transaction receipt.
                tx_receipt = self.connection.eth.wait_for_transaction_receipt(
                    txHash)  # waitForTransactionReceipt: Deprecated: This method is deprecated in favor of wait_for_transaction_receipt()
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
            # counter = self.connection.eth.get_transaction_count(address)
            ret_val, contract_instance = self.get_contract_instance()
            counter = contract_instance.functions.transaction_count().call()
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
            self.nonce_values[key] += 1  # set on next
            nonce = self.nonce_values[key]
        else:
            nonce = self.connection.eth.get_transaction_count(key)
            self.nonce_values[key] = nonce
        return nonce
