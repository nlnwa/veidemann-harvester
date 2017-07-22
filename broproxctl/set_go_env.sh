#!/bin/bash

# Script to set the GO environment to the same values as used by the mvn-golang-wrapper plugin.
# This way it is possible do developement without the need to run maven for every build.
# Note that you should run mvn package once to set up dependencies first.
#
# Usage:
#   mvn package
#   source ./set_go_env.sh
#   go install broproxctl

ENV=$(mvn mvn-golang-wrapper:custom | grep -e GOBIN -e GOPATH -e GOROOT | cut -d' ' -f2 | tr -d '"')

for e in ${ENV}; do
  export ${e}
done

export PATH=${PATH}:${GOROOT}/bin:${GOBIN}
