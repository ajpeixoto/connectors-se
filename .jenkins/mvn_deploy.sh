#!/usr/bin/env bash

set -xe

# Deploys on Nexus
# Maven phases validate to install are skipped with DEPLOY profile in pom
# $@: the extra parameters to be used in the maven command
main() {
  local extraBuildParams=("$@")

  mvn deploy \
    --errors \
    --batch-mode \
    --activate-profiles 'DEPLOY' \
    "${extraBuildParams[@]}"
}

main "$@"
