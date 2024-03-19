FROM python:3.9-alpine as base

# declare params
ENV EDGELAKE_PATH=/app \
    EDGELAKE_HOME=/app/EdgeLake \
    BLOCKCHAIN_DIR=/app/EdgeLake/blockchain \
    DATA_DIR=/app/EdgeLake/data \
    LOCAL_SCRIPTS=/app/deployment-scripts/node-deployment \
    TEST_DIR=/app/deployment-scripts/test \
    DEBIAN_FRONTEND=noninteractive \
    NODE_TYPE=generic

WORKDIR $EDGELAKE_PATH

COPY . EdgeLake
COPY setup.cfg $EDGELAKE_PATH

EXPOSE $ANYLOG_SERVER_PORT $ANYLOG_REST_PORT $ANYLOG_BROKER_PORT

RUN apk update && \
    apk upgrade && \
    apk add --no-cache bash git openssh-client gcc python3-dev musl-dev && \
    apk add bash python3 python3-dev py3-pip wget build-base libffi-dev py3-psutil && \
    apk add --no-cache bash git openssh-client gcc python3-dev build-base libffi-dev musl-dev && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade -r /app/EdgeLake/requirements.txt && \
    python3 -m pip install --upgrade pip wheel pyinstaller cython && \
    python3 /app/EdgeLake/setup_exe.py install && \
    git clone -b open-source https://github.com/AnyLog-co/deployment-scripts/ && \
    rm -rf  *.spec build/ EdgeLake && mkdir EdgeLake && mv ${EDGELAKE_PATH}/setup.cfg EdgeLake/setup.cfg

FROM base AS deployment
ENTRYPOINT ${EDGELAKE_PATH}/dist/edgelake process deployment-scripts/node-deployment/main.al