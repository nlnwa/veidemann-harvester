#!/usr/bin/env bash

set -ev

mvn -B -Pdocker-build package
