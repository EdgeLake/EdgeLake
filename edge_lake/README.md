The following contains the source code to run the different components 

The AnyLog software is installed on each node that participates in the AnyLog Network. Using configuration files, the node is configured to support one or more roles:

A ***Publisher Node*** in the network streams data generated from devices (or applications) to Operator Nodes for storage. The publisher can reside on a gateway or a server that collects the data from the devices and is next to the devices. A Publisher operates as follows: It constantly monitors a predetermined directory space. When file is added to the directory, the file is sent to an Operator Node to be hosted. The file is in JSON format and the file name is in a predetermined structure that provides additional information to the information contained in the JSON structure.

An ***Operator Node*** hosts the data received from one or more Publishers. The default storage is with PostgreSQL. The Operator receives the JSON data and loads the data to the local database. When queries are issued, the relevant operators participate in the query process.

A ***Query Node*** publish a REST API. Using the API, users and applications can issue SQL queries to the network and the reply is organized as a JSON object.

The ***Metadata*** is stored on a blockchain or a master node or locally.
