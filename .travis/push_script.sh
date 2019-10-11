#!/usr/bin/env bash

set -ev

mvn -B -Pdocker-build-and-push -Ddocker.tag="${1:-latest}"
