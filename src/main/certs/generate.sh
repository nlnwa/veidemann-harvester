#!/bin/bash

# default value assumes fedora
# (ubuntu: /usr/lib/ssl/openssl.cnf)
CONFIG=${OPENSSL_CONF:-/etc/pki/tls/openssl.cnf}
MINIKUBE_IP=`minikube ip`

echo "Using config: $CONFIG"
echo "Using IP: $MINIKUBE_IP"

[ -f tls.key ] && rm tls.key
[ -f tls.crt ] && rm tls.crt

# generate self signed certificate for use with veidemannctl or minikube (nginx ingress controller / linkerd)
openssl req \
        -newkey rsa:2048 \
        -x509 \
        -nodes \
        -keyout tls.key \
        -new \
        -out tls.crt \
        -subj /CN=$MINIKUBE_IP \
        -reqexts SAN -extensions SAN \
        -config <(cat $CONFIG <(printf "[SAN]\nsubjectAltName=IP:$MINIKUBE_IP")) \
        -sha256 \
        -days 1
