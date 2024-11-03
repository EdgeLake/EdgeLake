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


abi_ = [{"anonymous": False,"inputs": [{"indexed": False,"internalType": "bool","name": "","type": "bool"}],"name": "delete_policy_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string[]","name": "","type": "string[]"}],"name": "get_all_policies_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string[]","name": "","type": "string[]"}],"name": "get_all_policy_ids_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string","name": "","type": "string"}],"name": "get_policy_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "address","name": "","type": "address"}],"name": "get_policy_owner_event","type": "event"},{"anonymous": False,"inputs": [{"indexed": False,"internalType": "string","name": "","type": "string"},{"indexed": False,"internalType": "string","name": "","type": "string"}],"name": "updated_policy_event","type": "event"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "deletePolicy","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"name": "getAllPolicies","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"name": "getAllPolicyIds","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "getPolicy","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"}],"name": "getPolicyOwner","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"},{"internalType": "string","name": "json","type": "string"}],"name": "insert","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"name": "transaction_count","outputs": [{"internalType": "uint256","name": "","type": "uint256"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "string","name": "policy_id","type": "string"},{"internalType": "string","name": "json","type": "string"}],"name": "updatePolicy","outputs": [],"stateMutability": "nonpayable","type": "function"}]

byte_code = "60806040525f6002553480156012575f80fd5b506118f1806100205f395ff3fe608060405234801561000f575f80fd5b5060043610610086575f3560e01c806360dd5f901161005957806360dd5f90146100d65780636c450e09146100f2578063a0e60c181461010e578063f7ff50e21461012a57610086565b806306e63ff81461008a5780632709a3f8146100a6578063457e5521146100c25780634b254924146100cc575b5f80fd5b6100a4600480360381019061009f9190610c3e565b610148565b005b6100c060048036038101906100bb9190610c3e565b610381565b005b6100ca610692565b005b6100d4610703565b005b6100f060048036038101906100eb9190610cbc565b61073d565b005b61010c60048036038101906101079190610cbc565b61082a565b005b61012860048036038101906101239190610cbc565b6108a7565b005b610132610b77565b60405161013f9190610d1f565b60405180910390f35b6003848460405161015a929190610d74565b908152602001604051809103902060020160149054906101000a900460ff16156101b9576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016101b090610e0c565b60405180910390fd5b5f828290918060018154018082558091505060019003905f5260205f20015f9091929091929091929091925091826101f292919061105b565b506001848490918060018154018082558091505060019003905f5260205f20015f90919290919290919290919250918261022d92919061105b565b50604051806080016040528083838080601f0160208091040260200160405190810160405280939291908181526020018383808284375f81840152601f19601f82011690508083019250505050505050815260200160015f805490506102939190611155565b81526020013373ffffffffffffffffffffffffffffffffffffffff16815260200160011515815250600385856040516102cd929190610d74565b90815260200160405180910390205f820151815f0190816102ee9190611192565b50602082015181600101556040820151816002015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555060608201518160020160146101000a81548160ff021916908315150217905550905050600160025f8282546103749190611261565b9250508190555050505050565b60038484604051610393929190610d74565b908152602001604051809103902060020160149054906101000a900460ff166103f1576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016103e8906112de565b60405180910390fd5b3373ffffffffffffffffffffffffffffffffffffffff166003858560405161041a929190610d74565b90815260200160405180910390206002015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16146104a1576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016104989061136c565b60405180910390fd5b604051806080016040528083838080601f0160208091040260200160405190810160405280939291908181526020018383808284375f81840152601f19601f82011690508083019250505050505050815260200160038686604051610507929190610d74565b90815260200160405180910390206001015481526020013373ffffffffffffffffffffffffffffffffffffffff1681526020016001151581525060038585604051610553929190610d74565b90815260200160405180910390205f820151815f0190816105749190611192565b50602082015181600101556040820151816002015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555060608201518160020160146101000a81548160ff02191690831515021790555090505081815f600387876040516105fd929190610d74565b908152602001604051809103902060010154815481106106205761061f61138a565b5b905f5260205f2001918261063592919061105b565b50600160025f8282546106489190611261565b925050819055507f4d00be5f2f42b7a46224e119195a7c9e9d6bd923a29acd6f54c11c8d134b9d988484848460405161068494939291906113f3565b60405180910390a150505050565b7f461d80627c49f4753a01d3483f64d15543e7c352228322e696fe43354d394ad35f6040516106c1919061157a565b60405180910390a17f2e7ce3229a7b2679f2ed621852adb9751e7b9c3103c699f1083de65c40309e8860016040516106f9919061157a565b60405180910390a1565b7f2e7ce3229a7b2679f2ed621852adb9751e7b9c3103c699f1083de65c40309e886001604051610733919061157a565b60405180910390a1565b6003828260405161074f929190610d74565b908152602001604051809103902060020160149054906101000a900460ff166107ad576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016107a4906115e4565b60405180910390fd5b5f600383836040516107c0929190610d74565b90815260200160405180910390206001015490507f17b88547e856ab10e485425d4da97aa7af183cb4c2ba8cd590edb964a812af165f82815481106108085761080761138a565b5b905f5260205f200160405161081d9190611683565b60405180910390a1505050565b7f21f82dfd7e71adb54ffba84028263297f5ae30c611b9c52d19e68b408c49ae7f6003838360405161085d929190610d74565b90815260200160405180910390206002015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1660405161089b91906116e2565b60405180910390a15050565b600382826040516108b9929190610d74565b908152602001604051809103902060020160149054906101000a900460ff16610917576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161090e906112de565b60405180910390fd5b600160025f8282546109299190611261565b925050819055505f60038383604051610943929190610d74565b908152602001604051809103902060020160146101000a81548160ff0219169083151502179055505f6003838360405161097e929190610d74565b90815260200160405180910390206001015490505f60015f805490506109a49190611155565b90505f81815481106109b9576109b861138a565b5b905f5260205f20015f83815481106109d4576109d361138a565b5b905f5260205f200190816109e89190611722565b505f600182815481106109fe576109fd61138a565b5b905f5260205f20018054610a1190610e8e565b80601f0160208091040260200160405190810160405280929190818152602001828054610a3d90610e8e565b8015610a885780601f10610a5f57610100808354040283529160200191610a88565b820191905f5260205f20905b815481529060010190602001808311610a6b57829003601f168201915b5050505050905082600382604051610aa09190611845565b9081526020016040518091039020600101819055508060018481548110610aca57610ac961138a565b5b905f5260205f20019081610ade9190611192565b505f805480610af057610aef61185b565b5b600190038181905f5260205f20015f610b099190610b7d565b90556001805480610b1d57610b1c61185b565b5b600190038181905f5260205f20015f610b369190610b7d565b90557f5939cc59f60051e79cb8108ce0404b75eab25be7ebff470256f6861b24cff9e06001604051610b6891906118a2565b60405180910390a15050505050565b60025481565b508054610b8990610e8e565b5f825580601f10610b9a5750610bb7565b601f0160209004905f5260205f2090810190610bb69190610bba565b5b50565b5b80821115610bd1575f815f905550600101610bbb565b5090565b5f80fd5b5f80fd5b5f80fd5b5f80fd5b5f80fd5b5f8083601f840112610bfe57610bfd610bdd565b5b8235905067ffffffffffffffff811115610c1b57610c1a610be1565b5b602083019150836001820283011115610c3757610c36610be5565b5b9250929050565b5f805f8060408587031215610c5657610c55610bd5565b5b5f85013567ffffffffffffffff811115610c7357610c72610bd9565b5b610c7f87828801610be9565b9450945050602085013567ffffffffffffffff811115610ca257610ca1610bd9565b5b610cae87828801610be9565b925092505092959194509250565b5f8060208385031215610cd257610cd1610bd5565b5b5f83013567ffffffffffffffff811115610cef57610cee610bd9565b5b610cfb85828601610be9565b92509250509250929050565b5f819050919050565b610d1981610d07565b82525050565b5f602082019050610d325f830184610d10565b92915050565b5f81905092915050565b828183375f83830152505050565b5f610d5b8385610d38565b9350610d68838584610d42565b82840190509392505050565b5f610d80828486610d50565b91508190509392505050565b5f82825260208201905092915050565b7f506f6c696379207769746820706f6c69637920696420616c72656164792065785f8201527f6973747300000000000000000000000000000000000000000000000000000000602082015250565b5f610df6602483610d8c565b9150610e0182610d9c565b604082019050919050565b5f6020820190508181035f830152610e2381610dea565b9050919050565b5f82905092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52604160045260245ffd5b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602260045260245ffd5b5f6002820490506001821680610ea557607f821691505b602082108103610eb857610eb7610e61565b5b50919050565b5f819050815f5260205f209050919050565b5f6020601f8301049050919050565b5f82821b905092915050565b5f60088302610f1a7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff82610edf565b610f248683610edf565b95508019841693508086168417925050509392505050565b5f819050919050565b5f610f5f610f5a610f5584610d07565b610f3c565b610d07565b9050919050565b5f819050919050565b610f7883610f45565b610f8c610f8482610f66565b848454610eeb565b825550505050565b5f90565b610fa0610f94565b610fab818484610f6f565b505050565b5b81811015610fce57610fc35f82610f98565b600181019050610fb1565b5050565b601f82111561101357610fe481610ebe565b610fed84610ed0565b81016020851015610ffc578190505b61101061100885610ed0565b830182610fb0565b50505b505050565b5f82821c905092915050565b5f6110335f1984600802611018565b1980831691505092915050565b5f61104b8383611024565b9150826002028217905092915050565b6110658383610e2a565b67ffffffffffffffff81111561107e5761107d610e34565b5b6110888254610e8e565b611093828285610fd2565b5f601f8311600181146110c0575f84156110ae578287013590505b6110b88582611040565b86555061111f565b601f1984166110ce86610ebe565b5f5b828110156110f5578489013582556001820191506020850194506020810190506110d0565b86831015611112578489013561110e601f891682611024565b8355505b6001600288020188555050505b50505050505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f61115f82610d07565b915061116a83610d07565b925082820390508181111561118257611181611128565b5b92915050565b5f81519050919050565b61119b82611188565b67ffffffffffffffff8111156111b4576111b3610e34565b5b6111be8254610e8e565b6111c9828285610fd2565b5f60209050601f8311600181146111fa575f84156111e8578287015190505b6111f28582611040565b865550611259565b601f19841661120886610ebe565b5f5b8281101561122f5784890151825560018201915060208501945060208101905061120a565b8683101561124c5784890151611248601f891682611024565b8355505b6001600288020188555050505b505050505050565b5f61126b82610d07565b915061127683610d07565b925082820190508082111561128e5761128d611128565b5b92915050565b7f506f6c69637920494420646f6573206e6f7420657869737400000000000000005f82015250565b5f6112c8601883610d8c565b91506112d382611294565b602082019050919050565b5f6020820190508181035f8301526112f5816112bc565b9050919050565b7f4f6e6c7920706f6c696379206f776e65722063616e2075706461746520706f6c5f8201527f6963790000000000000000000000000000000000000000000000000000000000602082015250565b5f611356602383610d8c565b9150611361826112fc565b604082019050919050565b5f6020820190508181035f8301526113838161134a565b9050919050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603260045260245ffd5b5f601f19601f8301169050919050565b5f6113d28385610d8c565b93506113df838584610d42565b6113e8836113b7565b840190509392505050565b5f6040820190508181035f83015261140c8186886113c7565b905081810360208301526114218184866113c7565b905095945050505050565b5f81549050919050565b5f82825260208201905092915050565b5f819050815f5260205f209050919050565b5f82825260208201905092915050565b5f815461147481610e8e565b61147e8186611458565b9450600182165f811461149857600181146114ae576114e0565b60ff1983168652811515602002860193506114e0565b6114b785610ebe565b5f5b838110156114d8578154818901526001820191506020810190506114b9565b808801955050505b50505092915050565b5f6114f48383611468565b905092915050565b5f600182019050919050565b5f6115128261142c565b61151c8185611436565b93508360208202850161152e85611446565b805f5b858110156115685784840389528161154985826114e9565b9450611554836114fc565b925060208a01995050600181019050611531565b50829750879550505050505092915050565b5f6020820190508181035f8301526115928184611508565b905092915050565b7f506f6c69637920646f6573206e6f7420657869737400000000000000000000005f82015250565b5f6115ce601583610d8c565b91506115d98261159a565b602082019050919050565b5f6020820190508181035f8301526115fb816115c2565b9050919050565b5f815461160e81610e8e565b6116188186610d8c565b9450600182165f811461163257600181146116485761167a565b60ff19831686528115156020028601935061167a565b61165185610ebe565b5f5b8381101561167257815481890152600182019150602081019050611653565b808801955050505b50505092915050565b5f6020820190508181035f83015261169b8184611602565b905092915050565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6116cc826116a3565b9050919050565b6116dc816116c2565b82525050565b5f6020820190506116f55f8301846116d3565b92915050565b5f8154905061170981610e8e565b9050919050565b5f819050815f5260205f209050919050565b818103611730575050611805565b611739826116fb565b67ffffffffffffffff81111561175257611751610e34565b5b61175c8254610e8e565b611767828285610fd2565b5f601f831160018114611794575f8415611782578287015490505b61178c8582611040565b8655506117fe565b601f1984166117a287611710565b96506117ad86610ebe565b5f5b828110156117d4578489015482556001820191506001850194506020810190506117af565b868310156117f157848901546117ed601f891682611024565b8355505b6001600288020188555050505b5050505050505b565b8281835e5f83830152505050565b5f61181f82611188565b6118298185610d38565b9350611839818560208601611807565b80840191505092915050565b5f6118508284611815565b915081905092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603160045260245ffd5b5f8115159050919050565b61189c81611888565b82525050565b5f6020820190506118b55f830184611893565b9291505056fea264697066735822122034184ceb7c74dc788bab120a3b8a601339a4fbc7434738acff1cb7036293fec264736f6c634300081a0033"

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
