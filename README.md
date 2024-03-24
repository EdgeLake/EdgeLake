![AnyLog Logo](other/anylog_logo.png)

# EdgeLake

Make your edge nodes members of a decentralized network of nodes, optimized to manage and monitor data and resources at the edge.
* Deploy EdgeLake instances on your nodes at the edge. 
* Enable services on each node. 
* Stream data from your PLCs, Sensors, and applications to the edge nodes. 
* Query the distributed data from a single point (as if the data is hosted in a centralized database). 
* Manage your edge resources from a single point (the network of nodes reflects a [Single System Image](https://en.wikipedia.org/wiki/Single_system_image))

## How it Works
* By deploying EdgeLake on a node, the node joins a decentralized, P2P network of nodes.
* Using a network protocol and a shared metadata layer, the nodes operate a single machine that is optimized to capture, host, manage and query data at the edge. 
* The nodes share a metadata layer. The metadata includes policies that describe the schemas of the data, the data distribution, the participating nodes, security and data ownerships and more. 
The shared metadata is hosted in one of the following:
  * A member node designated as a Master Node.
  * A blockchain (making the network fully decentralized).
* Each node in the network is configured to provide data services. Examples of services:
  * Capture data via REST, MQTT, gRPC, JSON Files.
  * Host data in a local database (like _SQLite_ or _PostgreSQL_, _MongoDB_).
  * Satisfy Queries.

When an application issues a query, it is delivered to one of the nodes in the network. This node serves as an orchestrator of the query and operates as follows:
Using the shared metadata, the node determines which are the target nodes that host the relevant data. The query is transferred to the target nodes and the replies from all the target nodes are aggregated dynamically and returned as a unified reply to the application. 
This process is similar to [MapReduce](https://en.wikipedia.org/wiki/MapReduce), whereas the target nodes are determined dynamically by the query and the shared metadata. Monitoring of resources operates in a similar way.

## Download and Install

Detailed directions for Install EdgeLke can be found in [docker-compose repository](https://github.com/EdgeLake/docker-compose)

**Prepare Node(s)**:
1. Install requirements
   * _Docker_
   * _docker-compose_
   * _Makefile_
```shell
sudo snap install docker
sudo apt-get -y install docker-compose 
sudo apt-get -y install make
 
# Grant non-root user permissions to use docker
USER=`whoami` 
sudo groupadd docker 
sudo usermod -aG docker ${USER} 
newgrp docker
```

2. Clone _docker-compose_ from EdgeLake repository
```shell
git clone https://github.com/EdgeLake/docker-compose
cd docker-compose
```

**Deploy AnyLog**:
3. Update `.env` configurations for the node(s) being deployed -- specifically _LEDGER_CONN_ for _Query_ and Operator Nodes  
   * [master node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/anylog_master.env)
   * [operator node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/anylog_operator.env)
   * [query node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/anylog_query.env)

```dotenv
#--- General ---
# Information regarding which AnyLog node configurations to enable. By default, even if everything is disabled, AnyLog starts TCP and REST connection protocols
NODE_TYPE=master
# Name of the AnyLog instance
NODE_NAME=anylog-master
# Owner of the AnyLog instance
COMPANY_NAME=New Company

#--- Networking ---
# Port address used by AnyLog's TCP protocol to communicate with other nodes in the network
ANYLOG_SERVER_PORT=32048
# Port address used by AnyLog's REST protocol
ANYLOG_REST_PORT=32049
# A bool value that determines if to bind to a specific IP and Port (a false value binds to all IPs)
TCP_BIND=false
# A bool value that determines if to bind to a specific IP and Port (a false value binds to all IPs)
REST_BIND=false

#--- Blockchain ---
# TCP connection information for Master Node
LEDGER_CONN=127.0.0.1:32048

#--- Advanced Settings ---
# Whether to automatically run a local (or personalized) script at the end of the process
DEPLOY_LOCAL_SCRIPT=false
```

2. Start Node using _makefile_
```shell
make up [NODE_TYPE]

# examples
make up master
make up operator
make up query
```

