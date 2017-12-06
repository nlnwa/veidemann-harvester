#!/usr/bin/env bash

DOCKER_TAG=latest

if [ -n "${TRAVIS_TAG}" ]; then
  DOCKER_TAG=${TRAVIS_TAG}
fi

mvn -B -Pdocker-build -Ddocker.tag="${DOCKER_TAG}" install;
