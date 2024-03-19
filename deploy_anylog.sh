#!/bin/bash

 Run nebula wihin the node
if [[ ${ENABLE_NEBULA} == true ]] && [[ ${OVERLAY_IP} ]]  && ( [[ ${IS_LIGHTHOUSE} == true ]] || ([[ ${LIGHTHOUSE_IP} ]] && [[ ${LIGHTHOUSE_NODE_IP} ]] )) ; then
  bash $ANYLOG_PATH/nebula/deploy_nebula.sh
fi

if [[ ${INIT_TYPE} == empty ]] || [[ ${INIT_TYPE} == raw ]] ; then
  export NODE_TYPE=generic
fi

# The following script decides how to start the docker instance based on $INIT_TYPE env variable
if [[ ${INIT_TYPE} == bash ]] ; then
  /bin/bash
elif [[ ${INIT_TYPE} == prod ]] && [[ ${APP_NAME} ]] ; then
  chmod +x ${ANYLOG_PATH}/${APP_NAME}
  ${ANYLOG_PATH}/${APP_NAME} process $ANYLOG_PATH/deployment-scripts/node-deployment/main.al
elif [[ ${INIT_TYPE} == prod ]] ; then
  python3 $ANYLOG_HOME/anylog.py  process $ANYLOG_PATH/deployment-scripts/node-deployment/main.al
else
  echo "Unknown deployment type: ${INIT_TYPE}. Cannot continue..."
  exit 1
fi
