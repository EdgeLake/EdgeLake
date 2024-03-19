import org.hyperledger.fabric.gateway.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Connect_info {

    public static boolean connected_ = false;
    public static Path walletDirectory_ = null;
    public static Wallet wallet_ = null;

    public static Identity identity_ = null;
    public static Path networkConfigFile_ = null; // Path to a common connection profile describing the network.
    public static Gateway.Builder builder_ = null; // Gateway connection used to access the network.
    public static Network network_ = null;
    public static Contract contract_ = null;

    public void set_wallet_dir( Path certificate_dir) {
        walletDirectory_ = certificate_dir;
    }
    public Path get_wallet_dir() {
        return walletDirectory_;
    }

    public void set_net_config_file( Path networkConfigFile) {
        // Path to a common connection profile describing the network.
        networkConfigFile_ = networkConfigFile;
    }
    public Path get_net_config_file() {
        return networkConfigFile_;
    }

    public void set_wallet( Wallet wallet) {
        // Path to a common connection profile describing the network.
        wallet_ = wallet;
    }
    public Wallet get_wallet() {
        return wallet_;
    }

    public void set_builder(Gateway.Builder builder){
        builder_ = builder;
    }

    public Gateway.Builder get_builder(){
        return builder_;
    }

    public void set_network(Network network){
        // Save the network connection
        network_ = network;
    }

    public Network get_network(){
        return network_;
    }


    public void set_contract(Contract contract){
        // Save the network connection
        contract_ = contract;
    }

    public Contract get_contract(){
        return contract_;
    }

    public void set_connected(boolean connected){
        // Save the network connection
        connected_ = connected;
    }

    public boolean is_connected(){
        return connected_;
    }

}
