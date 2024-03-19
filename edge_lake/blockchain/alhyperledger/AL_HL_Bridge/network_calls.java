import org.hyperledger.fabric.gateway.*;
import org.hyperledger.fabric.gateway.impl.ContractImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

/*
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
*/

// Documentation: https://github.com/hyperledger/fabric-gateway-java/blob/main/README.md

public class network_calls {

    private static Connect_info conn_info_ = new Connect_info();


    /*
     * Return True is a contract is assigned to the connection
     */
    public static String get_contract_name() throws IOException{

        String contract_name;
        Contract contract = conn_info_.get_contract();

        if (contract != null){
            contract_name = ((ContractImpl) contract).getChaincodeId();
        }else{
            contract_name = "";
        }

        return contract_name;

    }
    /*
    * Flag connection established. This is called after the Java object in the Python code was called without an error
    */
    public static boolean set_connection(String certificate_dir, String network_config_file, String network_name, String contract_name) throws IOException{

        boolean ret_val;



        conn_info_.set_wallet_dir(Paths.get(certificate_dir));

        Path walletDirectory = conn_info_.get_wallet_dir();

        Wallet wallet = Wallets.newFileSystemWallet(walletDirectory);

        conn_info_.set_wallet( wallet );


        // Path to a common connection profile describing the network.
        conn_info_.set_net_config_file(Paths.get(network_config_file));
        Path networkConfigFile = conn_info_.get_net_config_file();

        // Configure the gateway connection used to access the network.
        Gateway.Builder builder = Gateway.createBuilder()
                                .identity(wallet, "Anylog")
                                .networkConfig(networkConfigFile);

        conn_info_.set_builder(builder);


        // Create a gateway connection

        Gateway gateway = builder.connect();

        if (gateway != null) {

            // Obtain a smart contract deployed on the network.
            Network network = gateway.getNetwork(network_name);
            conn_info_.set_network(network);

            Contract contract = network.getContract(contract_name);
            conn_info_.set_contract(contract);

            ret_val = true;
        }else{
            ret_val = false;
        }

        conn_info_.set_connected(ret_val);
        return ret_val;
    }

    public boolean is_connected(){
        return conn_info_.is_connected();
    }


    /*
     *  Add a new policy
     *  @param policy_id - the hash value of the policy
     *  @param  policy - the policy info
     *  @return - true(success)/false(error)
     */
    public static String insert_policy(String policy_key, String policy) throws IOException {
        String ret_message;

        Gateway.Builder builder = conn_info_.get_builder();
        Contract contract = conn_info_.get_contract();

        // Create a gateway connection
        try (Gateway gateway = builder.connect()) {

            // Submit transactions that store state to the ledger.
            byte[] createCarResult = contract.createTransaction("Insert")
                    .submit(policy_key, policy);
            System.out.println(new String(createCarResult, StandardCharsets.UTF_8));
            ret_message = "";
        }
        catch (ContractException | TimeoutException | InterruptedException e) {
            ret_message = ((ContractException) e).getMessage(); // Return the error message
        }
        return ret_message;
    }

    /*
     *  Add a new policy
     *  @param policy_id - the hash value of the policy
     *  @param  policy - the policy info
     *  @return - true(success)/false(error)
     */
    public static String delete_policy(String policy_key) throws IOException {
        String ret_message;
        Gateway.Builder builder = conn_info_.get_builder();
        Contract contract = conn_info_.get_contract();

        // Create a gateway connection
        try (Gateway gateway = builder.connect()) {

            // Submit transactions that store state to the ledger.
            byte[] deletePolicy = contract.createTransaction("DeletePolicy")
                    .submit(policy_key);

            System.out.println(new String(deletePolicy, StandardCharsets.UTF_8));
            ret_message = "";
        }
        catch (ContractException | TimeoutException | InterruptedException e) {
            ret_message = ((ContractException) e).getMessage(); // Return the error message
        }
        return ret_message;
    }

    /*
     *  Get all policies
     *  @return - all policies as a JSON string, or empty string if failed or no policies
     */
    public static String get_all() throws IOException {
        Gateway.Builder builder = conn_info_.get_builder();
        Contract contract = conn_info_.get_contract();

        // Create a gateway connection
        try (Gateway gateway = builder.connect()) {

            byte[] all_policies = contract.evaluateTransaction("GetAll");
            String ret = new String(all_policies, StandardCharsets.UTF_8);
            System.out.println(new String(all_policies, StandardCharsets.UTF_8));
            return ret;
        }
        catch (ContractException e) {
            return null;
        }
    }

}
