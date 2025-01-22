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

abi_ = [{"anonymous": False,"inputs": [{"indexed": False,"internalType": "bool","name": "","type": "bool"}],"name": "delete_policy_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string","name": "","type": "string"}],"name": "get_policy_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "address","name": "","type": "address"}],"name": "get_policy_owner_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string","name": "","type": "string"},{"indexed": False,"internalType": "string","name": "","type": "string"}],"name": "updated_policy_event","type": "event"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "deletePolicy","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "getPolicy","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "getPolicyOwner","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"},{"internalType": "string","name": "json","type": "string"}],"name": "insert","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"name": "num_policies","outputs": [{"internalType": "uint256","name": "","type": "uint256"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "uint256","name": "","type": "uint256"}],"name": "policies","outputs": [{"internalType": "string","name": "","type": "string"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "uint256","name": "","type": "uint256"}],"name": "policy_ids","outputs": [{"internalType": "string","name": "","type": "string"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "transaction_count","outputs": [{"internalType": "uint256","name": "","type": "uint256"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"},{"internalType": "string","name": "json","type": "string"}],"name": "updatePolicy","outputs": [],"stateMutability": "nonpayable","type": "function"}]

byte_code = "60806040525f6002555f6003553480156016575f80fd5b5061195f806100245f395ff3fe608060405234801561000f575f80fd5b5060043610610091575f3560e01c8063a0e60c1811610064578063a0e60c1814610105578063b13681dc14610121578063cb5a56c714610151578063d3e894831461016f578063f7ff50e21461019f57610091565b806306e63ff8146100955780632709a3f8146100b157806360dd5f90146100cd5780636c450e09146100e9575b5f80fd5b6100af60048036038101906100aa9190610d6d565b6101bd565b005b6100cb60048036038101906100c69190610d6d565b61040f565b005b6100e760048036038101906100e29190610deb565b610702565b005b61010360048036038101906100fe9190610deb565b6107ef565b005b61011f600480360381019061011a9190610deb565b61086c565b005b61013b60048036038101906101369190610e69565b610b55565b6040516101489190610f04565b60405180910390f35b610159610bfb565b6040516101669190610f33565b60405180910390f35b61018960048036038101906101849190610e69565b610c01565b6040516101969190610f04565b60405180910390f35b6101a7610ca6565b6040516101b49190610f33565b60405180910390f35b600484846040516101cf929190610f88565b908152602001604051809103902060020160149054906101000a900460ff161561022e576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161022590611010565b60405180910390fd5b5f828290918060018154018082558091505060019003905f5260205f20015f90919290919290919290919250918261026792919061125f565b506001848490918060018154018082558091505060019003905f5260205f20015f9091929091929091929091925091826102a292919061125f565b50604051806080016040528083838080601f0160208091040260200160405190810160405280939291908181526020018383808284375f81840152601f19601f82011690508083019250505050505050815260200160015f805490506103089190611359565b81526020013373ffffffffffffffffffffffffffffffffffffffff1681526020016001151581525060048585604051610342929190610f88565b90815260200160405180910390205f820151815f019081610363919061138c565b50602082015181600101556040820151816002015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555060608201518160020160146101000a81548160ff021916908315150217905550905050600160035f8282546103e9919061145b565b92505081905550600160025f828254610402919061145b565b9250508190555050505050565b60048484604051610421929190610f88565b908152602001604051809103902060020160149054906101000a900460ff1661047f576040517f08c379a0000000000000000000000000000000000000000000000000000000008152600401610476906114d8565b60405180910390fd5b3373ffffffffffffffffffffffffffffffffffffffff16600485856040516104a8929190610f88565b90815260200160405180910390206002015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff161461052f576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161052690611566565b60405180910390fd5b5f60048585604051610542929190610f88565b9081526020016040518091039020600101549050604051806080016040528084848080601f0160208091040260200160405190810160405280939291908181526020018383808284375f81840152601f19601f8201169050808301925050505050505081526020018281526020013373ffffffffffffffffffffffffffffffffffffffff16815260200160011515815250600486866040516105e5929190610f88565b90815260200160405180910390205f820151815f019081610606919061138c565b50602082015181600101556040820151816002015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555060608201518160020160146101000a81548160ff02191690831515021790555090505082825f838154811061068f5761068e611584565b5b905f5260205f200191826106a492919061125f565b50600160035f8282546106b7919061145b565b925050819055507f4d00be5f2f42b7a46224e119195a7c9e9d6bd923a29acd6f54c11c8d134b9d98858585856040516106f394939291906115dd565b60405180910390a15050505050565b60048282604051610714929190610f88565b908152602001604051809103902060020160149054906101000a900460ff16610772576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161076990611660565b60405180910390fd5b5f60048383604051610785929190610f88565b90815260200160405180910390206001015490507f17b88547e856ab10e485425d4da97aa7af183cb4c2ba8cd590edb964a812af165f82815481106107cd576107cc611584565b5b905f5260205f20016040516107e291906116ff565b60405180910390a1505050565b7f21f82dfd7e71adb54ffba84028263297f5ae30c611b9c52d19e68b408c49ae7f60048383604051610822929190610f88565b90815260200160405180910390206002015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051610860919061175e565b60405180910390a15050565b6004828260405161087e929190610f88565b908152602001604051809103902060020160149054906101000a900460ff166108dc576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016108d3906114d8565b60405180910390fd5b600160035f8282546108ee919061145b565b925050819055505f60048383604051610908929190610f88565b908152602001604051809103902060020160146101000a81548160ff0219169083151502179055505f60048383604051610943929190610f88565b90815260200160405180910390206001015490505f60015f805490506109699190611359565b90505f818154811061097e5761097d611584565b5b905f5260205f20015f838154811061099957610998611584565b5b905f5260205f200190816109ad919061179e565b505f600182815481106109c3576109c2611584565b5b905f5260205f200180546109d690611092565b80601f0160208091040260200160405190810160405280929190818152602001828054610a0290611092565b8015610a4d5780601f10610a2457610100808354040283529160200191610a4d565b820191905f5260205f20905b815481529060010190602001808311610a3057829003601f168201915b5050505050905082600482604051610a6591906118b3565b9081526020016040518091039020600101819055508060018481548110610a8f57610a8e611584565b5b905f5260205f20019081610aa3919061138c565b505f805480610ab557610ab46118c9565b5b600190038181905f5260205f20015f610ace9190610cac565b90556001805480610ae257610ae16118c9565b5b600190038181905f5260205f20015f610afb9190610cac565b90557f5939cc59f60051e79cb8108ce0404b75eab25be7ebff470256f6861b24cff9e06001604051610b2d9190611910565b60405180910390a1600160025f828254610b479190611359565b925050819055505050505050565b60018181548110610b64575f80fd5b905f5260205f20015f915090508054610b7c90611092565b80601f0160208091040260200160405190810160405280929190818152602001828054610ba890611092565b8015610bf35780601f10610bca57610100808354040283529160200191610bf3565b820191905f5260205f20905b815481529060010190602001808311610bd657829003601f168201915b505050505081565b60025481565b5f8181548110610c0f575f80fd5b905f5260205f20015f915090508054610c2790611092565b80601f0160208091040260200160405190810160405280929190818152602001828054610c5390611092565b8015610c9e5780601f10610c7557610100808354040283529160200191610c9e565b820191905f5260205f20905b815481529060010190602001808311610c8157829003601f168201915b505050505081565b60035481565b508054610cb890611092565b5f825580601f10610cc95750610ce6565b601f0160209004905f5260205f2090810190610ce59190610ce9565b5b50565b5b80821115610d00575f815f905550600101610cea565b5090565b5f80fd5b5f80fd5b5f80fd5b5f80fd5b5f80fd5b5f8083601f840112610d2d57610d2c610d0c565b5b8235905067ffffffffffffffff811115610d4a57610d49610d10565b5b602083019150836001820283011115610d6657610d65610d14565b5b9250929050565b5f805f8060408587031215610d8557610d84610d04565b5b5f85013567ffffffffffffffff811115610da257610da1610d08565b5b610dae87828801610d18565b9450945050602085013567ffffffffffffffff811115610dd157610dd0610d08565b5b610ddd87828801610d18565b925092505092959194509250565b5f8060208385031215610e0157610e00610d04565b5b5f83013567ffffffffffffffff811115610e1e57610e1d610d08565b5b610e2a85828601610d18565b92509250509250929050565b5f819050919050565b610e4881610e36565b8114610e52575f80fd5b50565b5f81359050610e6381610e3f565b92915050565b5f60208284031215610e7e57610e7d610d04565b5b5f610e8b84828501610e55565b91505092915050565b5f81519050919050565b5f82825260208201905092915050565b8281835e5f83830152505050565b5f601f19601f8301169050919050565b5f610ed682610e94565b610ee08185610e9e565b9350610ef0818560208601610eae565b610ef981610ebc565b840191505092915050565b5f6020820190508181035f830152610f1c8184610ecc565b905092915050565b610f2d81610e36565b82525050565b5f602082019050610f465f830184610f24565b92915050565b5f81905092915050565b828183375f83830152505050565b5f610f6f8385610f4c565b9350610f7c838584610f56565b82840190509392505050565b5f610f94828486610f64565b91508190509392505050565b7f506f6c696379207769746820706f6c69637920696420616c72656164792065785f8201527f6973747300000000000000000000000000000000000000000000000000000000602082015250565b5f610ffa602483610e9e565b915061100582610fa0565b604082019050919050565b5f6020820190508181035f83015261102781610fee565b9050919050565b5f82905092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52604160045260245ffd5b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602260045260245ffd5b5f60028204905060018216806110a957607f821691505b6020821081036110bc576110bb611065565b5b50919050565b5f819050815f5260205f209050919050565b5f6020601f8301049050919050565b5f82821b905092915050565b5f6008830261111e7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff826110e3565b61112886836110e3565b95508019841693508086168417925050509392505050565b5f819050919050565b5f61116361115e61115984610e36565b611140565b610e36565b9050919050565b5f819050919050565b61117c83611149565b6111906111888261116a565b8484546110ef565b825550505050565b5f90565b6111a4611198565b6111af818484611173565b505050565b5b818110156111d2576111c75f8261119c565b6001810190506111b5565b5050565b601f821115611217576111e8816110c2565b6111f1846110d4565b81016020851015611200578190505b61121461120c856110d4565b8301826111b4565b50505b505050565b5f82821c905092915050565b5f6112375f198460080261121c565b1980831691505092915050565b5f61124f8383611228565b9150826002028217905092915050565b611269838361102e565b67ffffffffffffffff81111561128257611281611038565b5b61128c8254611092565b6112978282856111d6565b5f601f8311600181146112c4575f84156112b2578287013590505b6112bc8582611244565b865550611323565b601f1984166112d2866110c2565b5f5b828110156112f9578489013582556001820191506020850194506020810190506112d4565b868310156113165784890135611312601f891682611228565b8355505b6001600288020188555050505b50505050505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f61136382610e36565b915061136e83610e36565b92508282039050818111156113865761138561132c565b5b92915050565b61139582610e94565b67ffffffffffffffff8111156113ae576113ad611038565b5b6113b88254611092565b6113c38282856111d6565b5f60209050601f8311600181146113f4575f84156113e2578287015190505b6113ec8582611244565b865550611453565b601f198416611402866110c2565b5f5b8281101561142957848901518255600182019150602085019450602081019050611404565b868310156114465784890151611442601f891682611228565b8355505b6001600288020188555050505b505050505050565b5f61146582610e36565b915061147083610e36565b92508282019050808211156114885761148761132c565b5b92915050565b7f506f6c69637920494420646f6573206e6f7420657869737400000000000000005f82015250565b5f6114c2601883610e9e565b91506114cd8261148e565b602082019050919050565b5f6020820190508181035f8301526114ef816114b6565b9050919050565b7f4f6e6c7920706f6c696379206f776e65722063616e2075706461746520706f6c5f8201527f6963790000000000000000000000000000000000000000000000000000000000602082015250565b5f611550602383610e9e565b915061155b826114f6565b604082019050919050565b5f6020820190508181035f83015261157d81611544565b9050919050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603260045260245ffd5b5f6115bc8385610e9e565b93506115c9838584610f56565b6115d283610ebc565b840190509392505050565b5f6040820190508181035f8301526115f68186886115b1565b9050818103602083015261160b8184866115b1565b905095945050505050565b7f506f6c69637920646f6573206e6f7420657869737400000000000000000000005f82015250565b5f61164a601583610e9e565b915061165582611616565b602082019050919050565b5f6020820190508181035f8301526116778161163e565b9050919050565b5f815461168a81611092565b6116948186610e9e565b9450600182165f81146116ae57600181146116c4576116f6565b60ff1983168652811515602002860193506116f6565b6116cd856110c2565b5f5b838110156116ee578154818901526001820191506020810190506116cf565b808801955050505b50505092915050565b5f6020820190508181035f830152611717818461167e565b905092915050565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6117488261171f565b9050919050565b6117588161173e565b82525050565b5f6020820190506117715f83018461174f565b92915050565b5f8154905061178581611092565b9050919050565b5f819050815f5260205f209050919050565b8181036117ac575050611881565b6117b582611777565b67ffffffffffffffff8111156117ce576117cd611038565b5b6117d88254611092565b6117e38282856111d6565b5f601f831160018114611810575f84156117fe578287015490505b6118088582611244565b86555061187a565b601f19841661181e8761178c565b9650611829866110c2565b5f5b828110156118505784890154825560018201915060018501945060208101905061182b565b8683101561186d5784890154611869601f891682611228565b8355505b6001600288020188555050505b5050505050505b565b5f61188d82610e94565b6118978185610f4c565b93506118a7818560208601610eae565b80840191505092915050565b5f6118be8284611883565b915081905092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603160045260245ffd5b5f8115159050919050565b61190a816118f6565b82525050565b5f6020820190506119235f830184611901565b9291505056fea2646970667358221220b6dd4907b80f3e19006e302b203660c9f0acda05fb103793d24beff9d941e1ae64736f6c634300081a0033"

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
    # Update data stored on the blockchain - Policy ID determines unique on the contract
    # -------------------------------------------------------------
    def update_policies(self, status, policy_id, policy):

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

                    transaction = contract_instance.functions.updatePolicy(policy_id, policy).build_transaction(
                        {
                            # 'gas': self.gas_write,     # 3172000
                            'chainId': chain_id,  # ,        # Goerli : 5  SEPOLIA : 11155111
                            # 'gasPrice': self.w3.toWei('1000000', 'wei'),
                            'nonce': nonce,
                            'from': self.public_key
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
                            'from': self.public_key
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
                    # Details at https://web3py.readthedocs.io/en/stable/web3.contract.html
                    log_policies = []
                    num_policies = contract_instance.functions.num_policies().call()
                    for i in range(0, num_policies):
                        log_policies.append(contract_instance.functions.policies(i).call())

                except:
                    data = None

                    errno, value = sys.exc_info()[:2]
                    err_msg = "Blockchain Failure: retrieve data from blockchain failed: {0} : {1}".format(str(errno),
                                                                                                           str(value))
                    status.add_error(err_msg)
                    ret_val = process_status.BLOCKCHAIN_operation_failed
                else:
                    if log_policies:
                        data = log_policies
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
        # return next nonce to use directly from RPC provider
        nonce = self.connection.eth.get_transaction_count(key,
                                                          "pending")  # adding "pending" will return mined txs + pending transactions. "latest" returns mined txs. https://docs.infura.io/api/networks/base/json-rpc-methods/eth_gettransactioncount#:~:text=eth_getTransactionCount.%20Returns%20the%20number%20of%20transactions%20sent%20from%20an%20address.
        self.nonce_values[key] = nonce
        return nonce
