#!/bin/bash


# Append or remove hostname and minikube ip to/from /etc/hosts

HOSTNAME=veidemann.local

if [[ $# -gt 0 ]] && [[ $1 -eq "undo" ]]; then
    sudo sed -i "/${HOSTNAME}/d" /etc/hosts
else
    echo "$(minikube ip) ${HOSTNAME}" | sudo tee -a /etc/hosts
fi
