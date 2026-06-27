# ==== Part 1: Build executable ====
FROM python:3.11-slim AS builder

WORKDIR /app/

# Copy source code
COPY . EdgeLake/

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends bash git openssh-client gcc python3-dev libffi-dev libopencv-dev && \
    python3 -m pip install --upgrade pip wheel pyyaml==6.0.2 pyinstaller && \
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
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /app/EdgeLake /app/EdgeLake/external_lib && \
    git clone -b pre-develop https://github.com/EdgeLake/deployment-scripts

# Copy only scripts/configs needed at runtime
COPY setup.cfg /app/EdgeLake/setup.cfg
COPY LICENSE /app/EdgeLake/LICENSE
COPY EdgeLake_Technical_Charter.pdf /app/EdgeLake/EdgeLake_Technical_Charter.pdf
COPY external_lib/* /app/EdgeLake/external_lib/

# ==== Part 3: Runtime configuration ====
ENV EDGELAKE_PATH=/app \
    EDGELAKE_HOME=/app/EdgeLake \
    APP_NAME=edgelake_agent \
    BLOCKCHAIN_DIR=/app/EdgeLake/blockchain \
    DATA_DIR=/app/EdgeLake/data \
    LOCAL_SCRIPTS=/app/deployment-scripts \
    TEST_DIR=/app/deployment-scripts/tests \
    DEBIAN_FRONTEND=noninteractive

ENTRYPOINT exec ${EDGELAKE_PATH}/${APP_NAME} process $EDGELAKE_PATH/deployment-scripts/node-deployment/main.al