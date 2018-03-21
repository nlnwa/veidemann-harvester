#!/bin/bash

set -e

CHART=${CHART:-../charts/veidemann}

function displayValues {
    echo ""
    echo "Installing veidemann ($RELEASE)"
    echo ""
    echo "CHART = $(realpath $CHART)"
    echo "VALUES = $(realpath $VALUES)"
    echo "NAMESPACE = $NAMESPACE"
    echo "CONTEXT = $(kubectl config current-context)"
    echo ""
}

function promptProceed {
    echo "Proceed? [y/N]"
    read PROCEED
    if [[ $PROCEED != "y" ]]; then
        exit
    fi
}

function preInstall {
    # Add incubator helm repo
    helm repo add incubator https://kubernetes-charts-incubator.storage.googleapis.com/

    # Update dependencies
    helm dep up --skip-refresh $CHART
}

function installMinikube {
    set -x

    # Install tiller on the server and wait for tiller to be ready
    helm init --wait

    preInstall

    # Install
    (MINIKUBE_IP=$(minikube ip) envsubst '${MINIKUBE_IP}' < $VALUES) | helm upgrade $RELEASE $CHART --install --namespace $NAMESPACE --values -
}

function installProd {
    set -x

    preInstall

    helm upgrade $RELEASE $CHART --install --namespace $NAMESPACE --values $VALUES "$@"
}


# Installing to production cluster requires a values.yaml file
if [[ $# -lt 1 ]]; then
    NAMESPACE=${NAMESPACE:-default}
    VALUES=${VALUES:-${CHART}/values.yaml}
    RELEASE=${RELEASE:-dev}

    # Enforce minikube context (not production)
    kubectl config use-context minikube

    displayValues
    promptProceed
    installMinikube

else
    NAMESPACE=${NAMESPACE:-nna}
    RELEASE=${RELEASE:-prod}
    VALUES=$1
    shift

    if [[ ! $(basename $VALUES) == "values.yaml" ]]; then
        echo "Parameter must be a path to a file named \"values.yaml\""
        exit
    fi

    displayValues
    promptProceed
    installProd "$@"
fi
