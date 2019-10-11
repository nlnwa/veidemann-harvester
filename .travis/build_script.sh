#!/usr/bin/env bash

DOCKER_TAG=${1:-latest}

mvn -B -Pdocker-build -Ddocker.tag="${DOCKER_TAG}" install;
