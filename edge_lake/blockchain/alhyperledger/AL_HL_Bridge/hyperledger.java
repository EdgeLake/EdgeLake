
//package org.example;

// based on example from https://www.py4j.org/

//import py4j.examples;
import org.apache.commons.io.filefilter.TrueFileFilter;
import py4j.GatewayServer;



public class hyperledger {


    public static void main(String[] args) {

        boolean is_test = false;

        if (is_test) {

            test_connection();

        }else{


            try {
                GatewayServer gatewayServer = new GatewayServer(new network_calls());
                gatewayServer.start();
                System.out.println("Java Gateway Server Started");

            } catch (Exception e) {
                System.out.println("Java Gateway Server Failed to Load");

            }

        }
    }

    private static void test_connection() {

//        String certificate_dir = "D:\\AnyLog-Code\\JasmyConfig\\msp\\Org2";
        String certificate_dir = "D:\\AnyLog-Code\\JasmyConfig\\wallet";
        String config_file = "D:\\AnyLog-Code\\JasmyConfig\\connection_profile.json";

        network_calls jasmy_calls = new network_calls();

        boolean ret_val;
        String ret_string;

        try {
            ret_val = jasmy_calls.set_connection(certificate_dir, config_file, "jasmy-dev", "anylog");

            ret_string = jasmy_calls.get_all();
        }catch (Exception e) {

            System.out.println("Java Gateway Server Failed");
            System.out.println(e.toString());

        }


    }

}


