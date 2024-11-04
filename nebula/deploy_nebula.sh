#!/bin/bash

# Download and untar nebula
cd $ANYLOG_PATH/nebula

export NEBULA_VERSION="v1.8.2"
export DOWNLOAD_URL="https://github.com/slackhq/nebula/releases/download/${NEBULA_VERSION}"

# Detect architecture
if [[ ! -e $ANYLOG_PATH/nebula/nebula.tar.gz ]] ; then
  ARCHITECTURE=$(uname -m)
  case $ARCHITECTURE in
    "x86_64")
      export DOWNLOAD_LINK="${DOWNLOAD_URL}/nebula-linux-amd64.tar.gz"
      ;;
    "aarch64")
      export DOWNLOAD_LINK="${DOWNLOAD_URL}/nebula-linux-arm64.tar.gz"
      ;;
    "armv7l")
      export DOWNLOAD_LINK="${DOWNLOAD_URL}/nebula-linux-arm-7.tar.gz"
      ;;
    *)
      echo "Unsupported architecture: $ARCHITECTURE"
      exit 1
      ;;
  esac
  wget "$DOWNLOAD_LINK" -O $ANYLOG_PATH/nebula/nebula.tar.gz
  tar -xzvf $ANYLOG_PATH/nebula/nebula.tar.gz
fi

#if [[ ${NEBULA_NEW_KEYS} == true ]] ; then
#  # archive original keys
#  mkdir -p $ANYLOG_PATH/nebula/archive_certs
#  mv $ANYLOG_PATH/nebula/ca.key $ANYLOG_PATH/nebula/archive_certs
#  mv $ANYLOG_PATH/nebula/ca.crt $ANYLOG_PATH/nebula/archive_certs

# create new keys
#  if [[ ${IS_LIGHTHOUSE} == true ]]; then
#    ./nebula-cert ca -name "${COMPANY_NAME}"
#  else
#    exit 1
#  fi
#fi

# create host keys if does not exist
if [[ -e $ANYLOG_PATH/nebula/configs/ca.crt ]] ; then
  mv $ANYLOG_PATH/nebula/configs/ca.crt .
fi
if [[ -e $ANYLOG_PATH/nebula/configs/ca.key ]] ; then
  mv $ANYLOG_PATH/nebula/configs/ca.key .
fi

if [[ ${IS_LIGHTHOUSE} == true ]]; then
  if [[ -e $ANYLOG_PATH/nebula/configs/lighthouse.crt ]] ; then
    mv  $ANYLOG_PATH/nebula/configs/lighthouse.crt $ANYLOG_PATH/nebula/host.crt
  fi
  if [[ -e $ANYLOG_PATH/nebula/configs/lighthouse.key ]] ; then
    mv  $ANYLOG_PATH/nebula/configs/lighthouse.key $ANYLOG_PATH/nebula/host.key
  fi
elif [[ ! -e $ANYLOG_PATH/nebula/host.crt ]] || [[ ! -e $ANYLOG_PATH/nebula/host.key ]] ; then
  export MASK=$(echo ${CIDR_OVERLAY_ADDRESS} | cut -d'/' -f2)
  ./nebula-cert sign \
    -name "host" \
    -ip "${OVERLAY_IP}/${MASK}" \
    -subnets "${CIDR_OVERLAY_ADDRESS}"
    rm -rf $ANYLOG_PATH/nebula/ca.key
fi

# Base command
CMD="python3 $ANYLOG_PATH/nebula/config_nebula.py ${CIDR_OVERLAY_ADDRESS} ${ANYLOG_SERVER_PORT} ${ANYLOG_REST_PORT}"
# Add options based on the conditions
if [[ -n ${ANYLOG_BROKER_PORT} ]]; then
    CMD+=" --broker-port ${ANYLOG_BROKER_PORT}"
fi
if [[ ${REMOTE_CLI} == true ]]; then
    CMD+=" --remote-cli"
fi
if [[ ${GRAFANA} == true ]]; then
    CMD+=" --grafana"
fi

# Check if it's a lighthouse node
if [[ ${IS_LIGHTHOUSE} == true ]]; then
    CMD+=" --is-lighthouse"
else
    CMD+=" --lighthouse-node-ip ${LIGHTHOUSE_NODE_IP}"
fi

# Execute the command
eval $CMD



rm -rf $ANYLOG_PATH/nebula/configs/

# start nebula
./nebula -config $ANYLOG_PATH/nebula/node.yml > $ANYLOG_PATH/nebula/nebula.log 2>&1 &

cd $ANYLOG_PATH/