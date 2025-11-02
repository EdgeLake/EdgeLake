FROM python:3.11-alpine as base

# declare params
ENV PYTHONPATH=/app/EdgeLake/ \
    EDGELAKE_PATH=/app \
    EDGELAKE_HOME=/app/EdgeLake \
    BLOCKCHAIN_DIR=/app/EdgeLake/blockchain \
    DATA_DIR=/app/EdgeLake/data \
    LOCAL_SCRIPTS=/app/deployment-scripts/node-deployment \
    TEST_DIR=/app/deployment-scripts/test \
    DEBIAN_FRONTEND=noninteractive \
    NODE_TYPE=generic \
    NODE_NAME=edgelake-node \
    COMPANY_NAME="New Company" \
    ANYLOG_SERVER_PORT=32548 \
    ANYLOG_REST_PORT=32549 \
    LEDGER_CONN=127.0.0.1:32049

WORKDIR /app

COPY . EdgeLake
COPY setup.cfg /app
COPY LICENSE /app
COPY README.md /app

EXPOSE $ANYLOG_SERVER_PORT $ANYLOG_REST_PORT $ANYLOG_BROKER_PORT

# Install dependencies
RUN apk update && apk upgrade && \
    apk add bash git gcc openssh-client python3 python3-dev py3-pip musl-dev build-base libffi-dev py3-psutil && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade -r /app/EdgeLake/requirements.txt && \
    git clone https://github.com/EdgeLake/deployment-scripts

FROM base AS deployment

# Make sure to set the EDGELAKE_HOME environment variable for Python explicitly
ENV EDGELAKE_HOME=/app/EdgeLake

# Use exec form of ENTRYPOINT to ensure the environment variables are passed correctly
# ENTRYPOINT ["/bin/sh"]
ENTRYPOINT python3 /app/EdgeLake/edge_lake/edgelake.py process /app/deployment-scripts/node-deployment/main.al
