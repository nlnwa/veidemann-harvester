#!/usr/bin/env bash

DOCKER_TAG=latest

if [[ -n "$TRAVIS_TAG" || "$TRAVIS_BRANCH" == "master" && "$TRAVIS_EVENT_TYPE" == "push" ]]; then
  if [[ -n "${TRAVIS_TAG}" ]]; then
    DOCKER_TAG=${TRAVIS_TAG}
  fi

  mvn -B -Pdocker-build-and-push -Ddocker.tag="$DOCKER_TAG" install;
fi
