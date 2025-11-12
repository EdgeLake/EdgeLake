# ==== Part 1: Build executable ====
FROM python:3.11-slim AS builder

WORKDIR /app/

# Step 1: Install system dependencies (cached unless Dockerfile changes)
RUN apt-get update && \
    apt-get install -y --no-install-recommends bash git openssh-client gcc python3-dev libffi-dev libopencv-dev

# Step 2: Copy only requirements.txt and install Python deps (cached unless requirements.txt changes)
COPY requirements.txt /app/EdgeLake/requirements.txt
RUN python3 -m pip install --upgrade pip wheel pyyaml==6.0.2 && \
    python3 -m pip install --upgrade -r /app/EdgeLake/requirements.txt

# Step 3: Copy source code (invalidates cache on ANY code change, but deps already installed)
COPY . /app/EdgeLake/

# Step 4: Build executable and cleanup
RUN cd /app/EdgeLake && \
    python3 setup.py install && \
    mv dist/* /app/edgelake_agent && \
    rm -rf EdgeLake build *.egg-info dist && \
    apt-get purge -y gcc python3-dev libffi-dev libopencv-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# ==== Part 2: Prepare runtime ====
FROM python:3.11-slim AS runtime

WORKDIR /app/

# Copy only the executable from builder
COPY --from=builder /app/edgelake_agent /app/edgelake_agent

# Copy MCP server directory from source (includes config files)
#COPY edge_lake/mcp_server /app/EdgeLake/edge_lake/mcp_server
COPY deploy_edgelake.sh /app/deploy_edgelake.sh
COPY setup.cfg /app/EdgeLake/setup.cfg


# Install runtime dependencies only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash ca-certificates python3-pip git dos2unix && \
    python3 -m pip install --no-cache-dir --upgrade \
        pip \
        grpcio-tools==1.70.0 \
        pyyaml==6.0.2 \
        requests==2.32.4 && \
    rm -rf /var/lib/apt/lists/* && \
    dos2unix /app/deploy_edgelake.sh

# Copy only scripts/configs needed at runtime
RUN mkdir -p /app/EdgeLake && \
    git clone -b os-dev https://github.com/Anylog-co/deployment-scripts.git


# ==== Part 3: Runtime configuration ====
ENV EDGELAKE_PATH=/app \
    EDGELAKE_HOME=/app/EdgeLake \
    APP_NAME=edgelake_agent \
    BLOCKCHAIN_DIR=/app/EdgeLake/blockchain \
    DATA_DIR=/app/EdgeLake/data \
    LOCAL_SCRIPTS=/app/deployment-scripts/node-deployment \
    TEST_DIR=/app/deployment-scripts/tests \
    DEBIAN_FRONTEND=noninteractive

RUN chmod +x /app/deploy_edgelake.sh
ENTRYPOINT ["/app/deploy_edgelake.sh"]
