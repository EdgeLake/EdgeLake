#!/bin/bash


if [[ -z ${APP_NAME} ]] ; then
  export VERSION=$(grep '^version' $EDGELAKE_HOME/setup.cfg | awk -F' = ' '{print $2}')
  ARCH=$(uname -m)
  case "$ARCH" in \
        x86_64) ARCH="x86_64" ;; \
        aarch64) ARCH="aarch64" ;; \
        *) ARCH="unknown" ;; \
    esac;
    export APP_NAME=edgelake_v${VERSION}_${ARCH}
    echo ${APP_NAME}
fi


if [[ -n "${HZN_DEVICE_ID}" ]]; then
  if [[ ! -n ${NODE_NAME} ]]; then export NODE_NAME=${HZN_DEVICE_ID} ; else export NODE_NAME=${NODE_NAME}_${HZN_DEVICE_ID} ; fi
fi

if [[ ! -d $EDGELAKE_PATH/deployment-scripts || ! "$(ls -A $EDGELAKE_PATH/deployment-scripts)" ]] ; then  # if directory DNE
  git clone https://github.com/EdgeLake/deployment-scripts
fi

if [[ ${ENABLE_NEBULA} == true ]] ; then
  if [[ ${NEBULA_NEW_KEYS} == true ]] && [[ -z ${CIDR_OVERLAY_ADDRESS} ]] ; then
    echo "Missing CIDR value, cannot create new keys..."
    export NEBULA_NEW_KEYS=false
  fi
    if [[ ${IS_LIGHTHOUSE} != true ]] && ([[ -z ${LIGHTHOUSE_IP} ]] || [[ -z ${LIGHTHOUSE_NODE_IP} ]]) ; then
    echo "Missing lighthouse IP information, cannot connect to Nebula network..."
    # remove overlay IP
    export OVERLAY_IP=""
  else
    bash $EDGELAKE_PATH/nebula/deploy_nebula.sh
  fi
fi

if [[ ${IS_KUBERNETES} == true ]] && [[ ${PROXY_IP} ]] ; then
  echo $(ifconfig eth0 | grep "inet addr" | awk -F ":" '{print $2}' | awk -F " " '{print $1}')  ${PROXY_IP}  >> /etc/hosts
fi

# The following script decides how to start the docker instance based on $INIT_TYPE env variable
chmod + ${EDGELAKE_PATH}/${APP_NAME}
if [[ ${INIT_TYPE} == bash ]] ; then
  /bin/bash
elif [[ ${INIT_TYPE} == prod ]] ; then
  if [[ ${APP_NAME} ]] && [[ ${DEBUG_MODE} -eq 2 ]] ; then
    ${EDGELAKE_PATH}/${APP_NAME} thread $EDGELAKE_PATH/deployment-scripts/node-deployment/main.al
  else
    ${EDGELAKE_PATH}/${APP_NAME} process $EDGELAKE_PATH/deployment-scripts/node-deployment/main.al
  fi
else
  echo "Unknown deployment type: ${INIT_TYPE}. Cannot continue..."
  exit 1
fi
