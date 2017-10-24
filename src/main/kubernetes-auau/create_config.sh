#!/bin/bash

if [ -z "$EXTERNAL_HOSTNAME" -o -z "$PILT_ADDR" ]; then
  echo "Two environment variables must be set:"
  echo " EXTERNAL_HOSTNAME - The host name where Veidemann is deployed"
  echo " PILT_ADDR         - URL to pilt"
  exit
fi

envsubst '${EXTERNAL_HOSTNAME} ${PILT_ADDR}' < configmap.template > configmap.yml
