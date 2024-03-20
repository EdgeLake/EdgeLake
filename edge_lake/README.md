The following contains the source code to run the different components 

The EdgeLake software is installed on each node that participates in the AnyLog Network. Using configuration files, the node is configured to support one or more roles:

An ***Operator Node*** hosts the data received from one or more Publishers. The default storage is with PostgreSQL. The Operator receives the JSON data and loads the data to the local database. When queries are issued, the relevant operators participate in the query process.

A ***Query Node*** publish a REST API. Using the API, users and applications can issue SQL queries to the network and the reply is organized as a JSON object.

The ***Metadata*** is stored on a blockchain or a master node or locally.
