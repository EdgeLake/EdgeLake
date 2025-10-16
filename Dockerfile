# ==== Part 1: Build executable ====
FROM python:3.11-slim AS builder

WORKDIR /app/

# Copy source code
COPY . EdgeLake/

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends bash git openssh-client gcc python3-dev libffi-dev libopencv-dev && \
    python3 -m pip install --upgrade pip wheel pyyaml==6.0.2 && \
    python3 -m pip install --upgrade -r /app/EdgeLake/requirements.txt && \
    python3 /app/EdgeLake/setup.py install && \
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

# Install runtime dependencies only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash ca-certificates python3-pip git && \
    python3 -m pip install --no-cache-dir --upgrade \
        pip \
        grpcio-tools==1.70.0 \
        pyyaml==6.0.2 \
        requests==2.32.4 && \
    rm -rf /var/lib/apt/lists/*

# Copy only scripts/configs needed at runtime
RUN mkdir -p /app/EdgeLake && \
    git clone https://github.com/oshadmon/nebula-anylog /app/nebula
COPY deploy_edgelake.sh /app/deploy_edgelake.sh
COPY setup.cfg /app/EdgeLake/setup.

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
