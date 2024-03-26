# EdgeLake

Make your edge nodes members of a decentralized network of nodes, optimized to manage and monitor data and resources at the edge.
* Deploy EdgeLake instances on your nodes at the edge. 
* Enable services on each node. 
* Stream data from your PLCs, Sensors, and applications to the edge nodes. 
* Query the distributed data from a single point (as if the data is hosted in a centralized database). 
* Manage your edge resources from a single point (the network of nodes reflects a [Single System Image](https://en.wikipedia.org/wiki/Single_system_image)).

### Table of Content
* [How it Works](#how-it-works)
* [Download & Install](#download-and-install)
* [Prerequisite & Setup Considerations](#prerequisite-and-setup-considerations)


## How it Works
* By deploying EdgeLake on a node, the node joins a decentralized, P2P network of nodes.
* Using a network protocol and a shared metadata layer, the nodes operate as a single machine that is optimized to capture, host, manage and query data at the edge. 
* The nodes share a metadata layer. The metadata includes policies that describe the schemas of the data, the data distribution, the participating nodes, security and data ownerships and more. 
The shared metadata is hosted in one of the following:
  * A member node designated as a Master Node.
  * A blockchain (making the network fully decentralized).
* Each node in the network is configured to provide data services. Examples of services:
  * Capture data via _REST_, _MQTT_, _gRPC_, _JSON Files_.
  * Host data in a local database (like _SQLite_ or _PostgreSQL_, _MongoDB_).
  * Satisfy Queries.

When an application issues a query, it is delivered to one of the nodes in the network. This node serves as an orchestrator of the query and operates as follows:
Using the shared metadata, the node determines which are the target nodes that host the relevant data. The query is transferred to the target nodes and the replies from all the target nodes are aggregated dynamically and returned as a unified reply to the application. 
This process is similar to [MapReduce](https://en.wikipedia.org/wiki/MapReduce), whereas the target nodes are determined dynamically by the query and the shared metadata. Monitoring of resources operates in a similar way.

Deploying an EdgeLake node and making the node a member of a network is done as follows:
* [Download and install](#download-and-install) the EdgeLake software on the Edge Node.
* Enable the services that determine the functionalities provided by the node. Services are enabled by one or a combination of the following:
    * Issuing configuration commands using the Node's Command Line Interface (CLI).
    * Listing configuration commands in a script file.
    * Listing configuration commands in policy that is hosted on the shared metadata.
    
The services configured determine the role of a node which can be one or a multiple of the following:
* **Operator Node** - a node that captures data and hosts the data on a local DBMS. Data sources like devices, PLCs and applications deliver data to Operator Nodes for storage. 
* **Query Node** - a node that orchestrates a query process. Applications deliver their queries to Query Nodes, these nodes interact with Operator Nodes (that host the data) to return a unified and complete reply for each query. 
* **Master Node** - a node that replaces a blockchain platform for storage of metadata policies. The network metadata is organized in Policies and users can associate a blockchain or alternatively a Master Node for metadata storage.

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

2. Clone _docker-compose_ repository from EdgeLake
```shell
git clone https://github.com/EdgeLake/docker-compose
cd docker-compose
```

**Deploy EdgeLake**:

3. Update `.env` configurations for the node(s) being deployed -- specifically _LEDGER_CONN_ for _Query_ and _Operator_ Nodes  
   * [master node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/edgelake_master.env)
   * [operator node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/edgelake_operator.env)
   * [query node](https://github.com/EdgeLake/docker-compose/tree/main/docker_makefile/edgelake_query.env)

```dotenv
#--- General ---
# Information regarding which EdgeLake node configurations to enable. By default, even if everything is disabled, EdgeLake starts TCP and REST connection services.
NODE_TYPE=master
# Name of the EdgeLake instance
NODE_NAME=anylog-master
# Owner of the EdgeLake instance
COMPANY_NAME=New Company

#--- Networking ---
# Port address used by EdgeLake's TCP protocol to communicate with other nodes in the network
ANYLOG_SERVER_PORT=32048
# Port address used by EdgeLake's REST protocol
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

## Prerequisite and Setup considerations
| Feature               | Requirement  |
| --------------------- | ------------| 
| Operating System      | Linux (Ubuntu, RedHat, Alpine, Suse), Windows, OSX |
| Memory footprint      | 100 MB available for EdgeLake deployed without Docker |
|                       | 300 MB available for EdgeLake deployed with Docker |
| Databases             | PostgreSQL installed (optional) |
|                       | SQLite (default, no need to install) |
|                       | MongoDB installed (Only if blob storage is needed) |
| CPU                   | Intel, ARM and AMD are supported. |
|                       | EdgeLake can be deployed on a single CPU machine and up to the largest servers (can be deployed on gateways, Raspberry PI, and all the way to the largest multi-core machines).|
| Storage               | EdgeLake supports horizontal scaling - nodes (and storage) are added dynamically as needed, therefore less complexity in scaling considerations. Requirements are based on expected volume and duration of data on each node. EdgeLake supports automated archival and transfer to larger nodes (if needed). |
| Network               | Required: a TCP based network (local TCP based networks, over the internet and combinations are supported) |
|                       | An overlay network is recommended. Most overlay networks can be used transparently. Nebula used as a default overlay network. |
|                       | Static IP and 3 ports open and accessible on each node (either via an Overlay Network, or without an Overlay). |
| Cloud Integration     | Build in integration using REST, Pub-Sub, and Kafka. |
| Deployment options    | Executable (can be deployed as a background process), or Docker or Kubernetes. |


**Comments**:
* Databases: 
  - SQLite recommended for smaller nodes and in-memory data.
  - PostgreSQL recommended for larger nodes.
  - MongoDB used for blob storage.
  - Multiple databases can be deployed and used on the same node.
    
* Network:
    An Overlay network is recommended for the following reasons:
    - Isolate the network for security considerations.
    - Manage IP and Ports availability. Without an overlay network, users needs to configure and manage availability 
      of IP and Ports used.
