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

WORKDIR $EDGELAKE_PATH

COPY . EdgeLake
COPY setup.cfg $EDGELAKE_PATH


RUN mkdir -p $EDGELAKE_PATH/nebula/configs/
COPY nebula/configs/* $EDGELAKE_PATH/nebula/configs/
COPY nebula/config.yml nebula/config_nebula.py nebula/deploy_nebula.sh nebula/export_nebula.sh nebula/validtae_ip_against_cidr.py $EDGELAKE_PATH/nebula/

EXPOSE $ANYLOG_SERVER_PORT $ANYLOG_REST_PORT $ANYLOG_BROKER_PORT

RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash git openssh-client gcc python3-dev musl-dev && \
    apk add bash python3 python3-dev py3-pip wget build-base libffi-dev py3-psutil && \
    apk add --no-cache bash git openssh-client gcc python3-dev build-base libffi-dev musl-dev && \
    python3 -m pip install --upgrade pip wheel pyinstaller>=0.0 Cython>=0.0 orjson && \
    python3 -m pip install --upgrade -r /app/EdgeLake/requirements.txt && \
    python3 /app/EdgeLake/setup.py install && \
    mv $EDGELAKE_PATH/dist/edgelake_v1.3.2411_x86_64 $EDGELAKE_PATH/dist/edgelake && \
    rm -rf `ls | grep -v dist  | grep -v nebula` && \
    mkdir -p $EDGELAKE_HOME $EDGELAKE_PATH/dco-signoff && \
    git clone -b os-dev https://github.com/AnyLog-co/deployment-scripts


FROM base AS deployment
WORKDIR $EDGELAKE_PATH

COPY setup.cfg $EDGELAKE_HOME
COPY LICENSE $EDGELAKE_HOME
COPY README.md $EDGELAKE_HOME
COPY requirements.txt $EDGELAKE_HOME
COPY dco-signoff $EDGELAKE_PATH/dco-signoff
COPY deploy_edgelake.sh  $EDGELAKE_PATH/

ENTRYPOINT /bin/bash $EDGELAKE_PATH/deploy_edgelake.sh
# ENTRYPOINT ${EDGELAKE_PATH}/dist/edgelake process deployment-scripts/node-deployment/main.al

