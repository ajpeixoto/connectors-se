#!/usr/bin/env bash

set -xe

# Deploys them on Nexus
# Maven phases validate to install are skipped with DEPLOY profile in pom
# $@: the extra parameters to be used in the maven command
main() {
  local extraBuildParams=("$@")

  # Maven phases:
  # xxx validate - validate the project is correct and all necessary information is available
  # xxx compile - compile the source code of the project
  # xxx test - test the compiled source code using a suitable unit testing framework
  # xxx package - take the compiled code and package it in its distributable format, such as a JAR
  # xxx verify - run any checks on results of integration tests to ensure quality criteria are met
  # xxx install - install the package into the local repository
  # >>> deploy - copies the final package to the remote repository

  mvn deploy \
    --errors \
    --batch-mode \
    --activate-profiles 'DEPLOY' \
    "${extraBuildParams[@]}"
}

main "$@"
