#!/bin/bash

if [[ -n "${HZN_DEVICE_ID}" ]]; then
  export NODE_NAME=${NODE_NAME}_${HZN_DEVICE_ID}
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

${EDGELAKE_PATH}/dist/edgelake process deployment-scripts/node-deployment/main.al
