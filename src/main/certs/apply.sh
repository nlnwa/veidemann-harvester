#!/bin/bash

KEY=tls.key
CERT=tls.crt
SECRET_NAME=ingress-certs

# check cert and key files exist
if [ ! -f $KEY ]; then echo "$KEY not found"; exit 1; fi
if [ ! -f $CERT ]; then echo "$CERT not found"; exit 1; fi

# delete existing secret if exists
kubectl get secret $SECRET_NAME && kubectl delete secret $SECRET_NAME

# create secret
kubectl create secret tls $SECRET_NAME --key $KEY --cert $CERT
