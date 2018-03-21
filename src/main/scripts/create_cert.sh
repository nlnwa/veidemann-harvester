#!/bin/bash

#######################################################
# Generate CA certificate and self-signed certificate #
#######################################################

SUBJECT="/C=NO/CN=veidemann.local"

# Generate CA signing key
openssl genrsa -out ca.key 2048

# Generate CA certificate and self-sign it
openssl req -new -x509 -days 365 -key ca.key -subj "/C=NO/CN=FakeAuthority" -out ca.crt

# Generate certificate signing request and private key for server certificate
openssl req -newkey rsa:2048 -nodes -keyout tls.key -subj "/CN=veidemann.local" -out tls.csr

# Sign server certificate with (self-signed) CA certificate and key
openssl x509 -req -extfile <(printf "subjectAltName=DNS:veidemann.local,DNS:localhost") -days 365 -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out tls.crt

#####################################################################################
# DEPRECATED:                                                                       #
# Self signed certificate without CA does not work for veidemann-controller because #
# Java library complains about subject alternative name not containing IP section   #
# even though it does.                                                              #
#####################################################################################

# default value assumes fedora
# (ubuntu: /usr/lib/ssl/openssl.cnf)
# CONFIG=${OPENSSL_CONF:-/etc/pki/tls/openssl.cnf}
# generate self signed certificate for use with veidemannctl or minikube (nginx ingress controller / linkerd)
# openssl req \
#         -newkey rsa:2048 \
#         -x509 \
#         -nodes \
#         -keyout tls.key \
#         -new \
#         -out tls.crt \
#         -subj /CN=$MINIKUBE_IP \
#         -reqexts SAN -extensions SAN \
#         -config <(cat $CONFIG <(printf "[SAN]\nsubjectAltName=IP:${MINIKUBE_IP}")) \
#         -sha256 \
#         -days 1
