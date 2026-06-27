---
title: Changelog
description: Release history and notable changes across all AnyLog Network versions.
layout: page
---
<!-- last-processed: 0b51657 -->

## 1.5.2604 (June 2026 · 0b51657 · 2026-06-13)

The primary change driving the version bump from 1.4 → 1.5 is the migration of the build process from PyInstaller + Cython
to Nuitka. 

No breaking API or protocol changes were introduced; all other fixes and improvements below are incremental.

### 🐛 Bug Fixes
- Fixed retrieve mapping for id 0 in Commands (A)
- Fixed import error in `ethereum.py` (Blockchain)
- Fixed error in `map_json_to_insert.py` L705 — when no mapping policy is provided, mapping now correctly transforms attribute-value pairs to column values (Blockchain / JSON→SQL mapping)
- Fixed error aggregations in Commands (AE)
- Fixed policy output and improved error messages in Core utilities (A)
- Fixed `.lower()` call on `protocol_name` when value is `None` in `message_server.py` (Networking)
- Fixed `requests.exception` → `requests.exceptions` typo (Networking)

### 🔧 Improvements
- **Modbus integration (A)**: Added UNS namespace parameter; added `dynamic=true` support; updated `read_data` parameters; added TCP automatic reconnect on errors; improved error handling; added Modbus PLC data ingestion
- **PLC integration (A)**: Added UNS namespace parameter; added `dynamic=true` support; added TCP automatic reconnect on errors; improved error handling; removed unused code; added URL validation for OPC-UA / EtherIP; improved help text (generic and REST-style); added Modbus PLC data ingestion
- **Commands (A)**: Added UNS namespace parameter; added `dynamic=true` support; added error handling for PLC client URL validation; improved help text; added Modbus PLC data ingestion
- **Core utilities (A)**: Added TCP automatic reconnect on errors; updated lazy import logic for Nuitka compatibility
- **Networking (A)**: Updated lazy import logic for method resolution
- **Node membership (A)**: Added clarifying comment
- **Database / SQL (A)**: Added debug instrumentation; updated lazy import logic

### ⚙️ Infrastructure & DevOps
- **Build system migrated from PyInstaller to Nuitka**: Docker images can now be built with either Nuitka (`Dockerfile-nuitka`) or Cython (`Dockerfile-basic`) via `image_builder.sh`
- Nuitka compilation validated across Camera integration, Commands, Core utilities, Database / SQL, Networking, gRPC, and Dependencies
- Dockerfile updated to include Nuitka support
- CI/CD workflows overhauled: nightly build, branch automation, version control for `ms-dev`, simplified workflow structure, removed over-engineered steps (#402 – #407)
- GitHub Actions updated to publish Docker images automatically
- Docker image logic updated for consistent image naming and build process (#414, #415)
- `setup.cfg` no longer required in deployment environments
- All modules updated to use lazy import logic for Nuitka compatibility
- Docker path and import issues resolved across environments
- Version control updated to support Modbus integration

---
## 1.4.2604 (April 2026 · acf221c · 2026-04-29)

### April 2026 (version 1.4.2604)

#### ✨ New Features
- **Python package inspection tool**: outputs installed packages in table or JSON format, with optional filtering by package name
- **MCP proxy layer**: routes and forwards MCP requests between nodes

#### 🐛 Bug Fixes
- Fixed MCP server tool descriptions for query and UNS operations
- Fixed POST routing bug in MCP proxy
- Fixed Grafana integration errors
- Fixed MQTT broker connection and message handling issues
- Fixed UNS pull info retrieval (`get_pull_info`)
- Fixed JSON POST message formatting
- Fixed string concatenation errors affecting query processing
- Fixed DBMS processing time calculation
- Fixed bug in reading scripts from policies
- Fixed `chown` location issue in Docker container startup
- Fixed PostgresSQL — stopped generating CSV output during `run immediate` commands
- Fixed blockchain bug in `pprint` output formatting
- Fixed messages to client formatting

#### 🔧 Improvements
- Dockerfile now displays `anylog@[node-name]-[hostname]` on `exec`, making it immediately clear when inside a container
- Cross-platform config switching: configs now cleanly toggle between Linux and Windows
- Removed redundant `chown` calls to streamline container initialization
- Reorganized `deploy_anylog` structure for cleaner builds
- Updated path resolution logic for more reliable cross-environment deployment
- Node scripts updated for consistency
- Set `User-Agent` header to `AnyLog-Agent` for consistent API identification
- TAG comments added for improved version tracking clarity
- Nicer CLI output formatting

#### ⚙️ Infrastructure & DevOps
- Workflow permission fixes and branch automation improvements
- Image builder updated to support Unix in addition to Linux
- Docker package hidden imports consolidated
- `setup.cfg` updated for packaging configuration
- when merging between os-dev & pre-develop there were workflow issues that were not taken into consideration
 
---

## 1.4.2603 (pre-develop · 8a98ca · 2026-03-16)

### ✨ New Features
- **REST API via message body**: AnyLog commands can now be sent via REST calls embedded in the HTTP message body without requiring custom headers — broadens client compatibility
- **MCP Server — AnyLog Command Relay**: MCP server can now reply with native AnyLog commands and handle POST requests, enabling richer AI-agent ↔ node interaction
- **REST `/api/status` endpoint**: New status call exposed via the REST interface
- **gRPC support for AI video inference**: Nodes can now receive and process AI inference results over gRPC streams, enabling real-time video analytics pipelines
- **Automated MQTT client**: MQTT client can now be configured and launched automatically from node policy, reducing manual setup steps
- **Multi-threaded MQTT client**: MQTT message ingestion now runs across a pool of threads, significantly improving throughput for high-volume message streams
- **MQTT message logging**: Incoming MQTT data is now logged for auditability and debugging
- **Aggregations in UNS and MCP**: Time-series aggregation functions are now available within both the Unified Namespace and MCP interfaces
- **PCAP data ingestion**: Added support for ingesting and processing PCAP (network packet capture) data
- **Semantic UNS policies**: UNS policies updated to support semantic Unified Namespace structure, including `source_node` tracking
- **TCP thread management**: Added configurable thread counts for TCP, REST, and Broker services; threads can now be gracefully terminated
- **Blockchain — get root policies**: New command to retrieve root-level blockchain policies
- **Policy indexing**: Indexes added to policy lookups for faster blockchain queries
- **MCP — get data nodes**: New MCP call to enumerate available data nodes in the network
- **MCP — `getChildPolicies`**: New MCP method to retrieve child policies from a parent policy context
- **UNS moved to Enterprise tier**: Unified Namespace functionality migrated to the enterprise version of AnyLog
- **Cluster policy excluded from namespace**: Cluster policies are no longer included in the UNS namespace to prevent conflicts
- **Node-RED flow updated**: Node-RED integration configuration updated to reflect current node architecture
- **Deployment script selection**: Node deployment now supports selecting from multiple deployment scripts at launch

### 🐛 Bug Fixes
- Fixed SQL parsing error with `AVG` aggregate queries
- Fixed MCP server — redundant trailing comma in generated responses
- Fixed MCP for Ollama integration — corrected API call format
- Fixed `blockchain get (operator,) bring.first` query returning incorrect results
- Fixed video plugin import errors
- Fixed aggregation edge cases in OPC-UA data flows
- Fixed `drop dbms` command bug
- Fixed queries using `include` directive returning wrong results
- Fixed JSON file reading in relational query context
- Fixed MQTT not starting correctly in PoC environments
- Fixed table names auto-generated from MQTT broker — now correctly forced to lowercase
- Fixed error in policy validation logic
- Fixed null handling in JSON-to-SQL mapping transformations
- Fixed null handling in general JSON processing
- Fixed uninitialized variable in message handling
- Fixed data type coercion between `int` and `date` types
- Fixed MQTT log write errors
- Fixed `str_to_list` transformation used in `bring` command mapping
- Fixed mapping policy bug with `__start__` / `__end__` script delimiters
- Fixed certificate handling in TLS/SSL connections
- Fixed SQL INSERT missing `NULL` values in generated statements
- Fixed aggregations crash when encountering `None` values
- Fixed aggregations value mismatch (reported externally)
- Fixed error message for malformed row structures
- Fixed quotation issue with license key handling

### 🔧 Improvements
- Exception handling improved: `logger` calls replaced with proper `exception` handlers for better error traceability
- `anylog docker build` updated for Python 3.13 compatibility
- gRPC client updated with crash guards and improved inline documentation
- `blockchain get` simplified — operator reads no longer require full policy fetch
- `bring.json` output — single quotes now correctly replaced with double quotes
- MCP server relays replies directly to the dashboard with `Connection` keep-alive headers
- Video streaming: added connection retry support and utility for writing JSON lists to file
- MCP thread count is now configurable and capped to prevent resource exhaustion
- Force-install flag added to requirements installation to prevent version conflicts
- `if/else` logic added for blockchain conditional policy evaluation
- Mapping policy naming convention standardized
- Workflow now supports version management automation
- Updated README with setup and usage documentation
- Proxy example added for MCP configuration reference

### ⚙️ Infrastructure & DevOps
- Docker builder updated and stabilized for multi-platform builds
- Docker home directory path resolution improved
- Docker paths corrected across environments
- Added `.gitignore` entries for log files and `__pycache__`
- `setup.cfg` updated for packaging metadata
- Synced `os-dev`, `ms-dev`, and `rs-dev` branches

---

## 2025 — Year in Review
The primary focus of 2025 was **edge deployment, containerization, and media integration**. Docker and Podman support 
was significantly expanded, with Makefile-driven build automation replacing manual image workflows. OPC-UA connectivity 
matured with improved async handling, and a new EtherIP protocol integration was added for industrial PLC communication. 
Video and camera pipeline support was introduced, enabling nodes to ingest and process media streams. Nebula-based overlay 
networking was stabilized for secure node-to-node communication. The aggregations engine was substantially extended. 
Akave decentralized storage was integrated as a new data sink, and remote deployment templates were improved to simplify 
multi-node rollouts.

| Date | Commit | Summary |
|------|--------|---------|
| 2025-11-30 | [845952] | (A) MCP Server — initial release |
|            |          | (A) Connector to Akave decentralized storage |
|            |          | (AE) Timezone support added to query casting options |
|            |          | (A) New command: `get mcp status` — info on connected MCP clients |
|            |          | (A) New command: `get node resources` — info on available node resources |
| 2025-10-18 | [2a53f6] | (A) Merge and join policies dynamically in a `blockchain get` command |
| 2025-09-24 | [fb9340] | (A) New command: `file from` — return a file via REST |
| 2025-08-16 | [69bf12] | (AE) New `childfrom` option in `where` condition to retrieve immediate child policies |
| 2025-08-04 | [77ce52] | (AE) New options for `get columns` command |
| 2025-07-26 | [cc7a3b] | (A) New command: `run helpers` — initiates AnyLog helper processes |
|            |          | (AE) New command: `get dynamic stats` — info on internal processes |
| 2025-07-11 | [318361] | (AE) New command: `flush buffers` — force streaming data to database ignoring thresholds |
| 2025-06-07 | [1c2753] | (AE) Windows Event Log management support |
| 2025-05-15 | [009f60] | (AE) `trace method [on/off] [method name]` — enable/disable tracing of specific methods |
|            |          | (AE) New command: `get nics list` — list all network interfaces |
|            |          | (AE) New command: `set internal ip with [nic name]` |
| 2025-05-12 | [168405] | (AE) `blockchain get` now supports `bring.list` to return lists of objects from metadata |
| 2025-05-05 | [d483ae] | (AE) EtherNet/IP connector — initial release |
|            |          | (AE) OPC-UA call updated to be consistent with EtherNet/IP interface |
| 2025-04-27 | [673f50] | (AE) `extend` and `include` options added to Grafana payload |
|            |          | (AE) Fix: reject tag policies (OPC-UA) with duplicate String ID or Int ID |
| 2025-04-12 | [3fd820] | (AE) Optimized increments function |
|            |          | (AE) New command: `get increments params` |
|            |          | (AE) New option to optimize data points returned in Grafana |
| 2025-03-30 | [7ec215] | (AE) New command: `set output table width 250` |
|            |          | (AE) New string substring operations: `!param_name[from_offset:to_offset]` |
|            |          | (AE) Severe error messages now printed in RED on the node CLI |
| 2025-03-07 | [d9321d] | (AE) DNS name support via `get dns name` command |
| 2025-02-16 | [d6c050] | (A) Aggregations over user data |
|            |          | (AE) New command: `subprocess` — run shell scripts from within AnyLog |
| 2025-01-26 | [6665e9] | (AE) `wait` command now supports waiting for blockchain sync to complete |
| 2025-01-04 | [139b43] | (A) License policy — formal license management via blockchain policy |
|            |          | (AE) Extended string operations: `!param_name[from_offset:to_offset]` substring syntax |
|            |          | (AE) `exit mqtt` replaced by `exit msg client [n/all]` |
| 2025-01-02 | [c03b82] | (AE) OPC-UA support — initial release |
 

---

## 2024 — Year in Review

2024 centered on **integrations, enterprise features, and observability**. Grafana dashboards were significantly improved 
with new query panels and data source configurations. OPC-UA support was introduced as a first-class data ingestion path. 
Async message processing was added to reduce latency on high-throughput nodes. The Nebula secure networking layer was 
introduced for overlay network management. License enforcement was tightened for enterprise deployments. The EdgeLake 
project emerged as a related effort, and mapping and transform rules were extended to handle more complex JSON-to-SQL 
conversion scenarios.

| Date | Commit | Summary |
|------|--------|---------|
| 2024-12-21 | [024a85] | (AE) New command: `file to` — write a file to a directory via CLI or REST |
|            |          | (A) `process` command now supported via REST PUT |
|            |          | (AE) File name structure in `file store` command is now optional |
| 2024-12-07 | [a4924f] | (AE) HTTP commands — specify AnyLog commands and output format via HTTP requests |
| 2024-08-29 | [be71d3] | (AE) New SQL casting functions: `function`, `lstrip`, `rstrip`, `timediff` |
|            |          | (AE) Increment function without specifying time range — intervals provided dynamically |
|            |          | (AE) Configurable number of threads for peer message sends |
|            |          | (AE) Monitor inserts via `trace level = 1 insert 10000` |
| —          | —        | (AE) EdgeLake branch created |
|            |          | (AE) New command: `get policies diff` — compare two policies |
|            |          | (AE) HTML document generation from query results |
|            |          | (AE) `unlog` option added to PSQL declaration |
|            |          | Updated `blockchain set account info` to include Chain ID |

---

## 2023 — Year in Review

2023 was dominated by **packaging, licensing, and operational stability**. The policy system was heavily refined, with 
improved validation, child policy traversal, and namespace management. Docker images were rebuilt on Alpine Linux to 
reduce footprint, and a compiled binary distribution was introduced alongside the source distribution. License 
management was formalized with enforcement hooks in the node startup sequence. Syslog ingestion was added as a native 
data source. Configuration management was streamlined, and a significant body of work went into improving error 
messages and CLI output for both developers and operators.

| Date | Commit | Summary |
|------|--------|---------|
| 2023-10-12 | —      | (AE) Sort by columns added to `blockchain get` command |
|            |        | (AE) Sort by columns added to `get data nodes` command |
| 2023-10-02 | —      | (AE) pip install support |
|            |        | (AE) Deploy AnyLog node as a background process |
|            |        | (AE) Map a local CLI to a peer node |
|            |        | (AE) Start a new node with a seed from a peer node |
|            |        | (A) `wait` command — pause execution by time and condition |
|            |        | (A) `create policy` command — declare policies with default attributes |
|            |        | (A) REST requests without a command now assume `get status` |
|            |        | (AE) Associate peer replies to a dictionary key |
|            |        | (A) `run blockchain sync` now retrieves connection info from Master Node policy if not provided |
|            |        | (A) `get status` now returns a warning if no license key is provided |
|            |        | (A) `min` and `max` options added to the `bring` command |
|            |        | (AE) Fix: `get inserts` was ignoring data inserted in immediate mode |
| —          | —      | (AE) gRPC support |
|            |        | (AE) SysLog support |
|            |        | (AE) New command: `delete archive` |
|            |        | (AE) `run mqtt client` deprecated — replaced by `run msg client` |

---

## 2022 — Year in Review

2022 focused on **deployment maturity, remote operations, and data management**. Docker volume management was overhauled 
to support persistent and configurable storage layouts. Remote node deployment — including over Grafana and REST — was a 
major theme, enabling operators to manage distributed nodes without direct shell access. Blob storage support was 
introduced for handling unstructured data alongside time-series. PostgreSQL was established as a production-grade 
backend alongside SQLite. Mapping policies were introduced to declaratively transform incoming data, and the 
operator/publisher model was refined for multi-node topologies.



---

## 2021 — Year in Review

2021 introduced **Ethereum blockchain integration and streaming data support**. Nodes could now anchor metadata and 
policy records to Ethereum, providing an immutable audit trail alongside the native AnyLog blockchain. Streaming 
ingestion was added as a complement to batch and REST-based data ingest. Authentication flows were improved with more 
robust session and token handling. Timezone-aware query processing was added, fixing a class of timestamp 
inconsistencies in distributed queries. Docker-based deployment was formalized and monitoring and alerting hooks were 
extended.

---

## 2020 — Year in Review

2020 was the year of **platform expansion and enterprise readiness**. The query engine was significantly extended with 
OLEDB connection support, enabling BI tools to query AnyLog nodes directly. Grafana integration was introduced as the 
primary visualization layer. An authentication framework was built to secure REST and TCP interfaces. Partition-based 
data management was added for time-series tables, improving query performance on large datasets. The operator, publisher, 
and consumer node roles were formalized into a coherent distributed processing model. SQLite was hardened as the default 
embedded database.

---

## 2019 — Year in Review

2019 was the **core platform build-out year**. The AnyLog query interface was designed and implemented, establishing the 
SQL-like command language that underpins all data access. The blockchain layer was introduced for decentralized policy 
and metadata management. A messaging and script system was built to allow nodes to exchange commands and execute 
policy-driven workflows. The client-server architecture was stabilized, and an extensive suite of integration tests was 
developed. Debugging infrastructure and trace logging were added to support early adopters.

---

## 2018 — Year in Review

The AnyLog Network project was founded in mid-2018. Initial commits established the core concepts: **producer nodes** 
that generate and publish sensor data, **contractor nodes** that receive and store it, and a lightweight **query layer** 
for retrieving records. Early work included sensor data generators producing JSON and SQL output, pseudo-code for the 
producer/contractor protocol, and foundational README documentation. The repository structure, GitHub configuration, and 
initial branching strategy were all put in place during this period.

---

*For the full commit-level history, run `git log` or browse the repository on GitHub.*