

// based on example from https://www.py4j.org/

//import py4j.examples;
import py4j.GatewayServer;



public class hyperledger {
   
    public static void main(String[] args) {

        try {
            GatewayServer gatewayServer = new GatewayServer(new hl_calls());
            gatewayServer.start();
            System.out.println("Java Gateway Server Started");

        }catch (Exception e) {
            System.out.println("Java Gateway Server Failed to Load");

        }
    }
 
}


