#!/usr/bin/env bash

DOCKER_TAG=${1:-latest}

mvn -B -Pdocker-build-and-push -Ddocker.tag="${DOCKER_TAG}" install;
