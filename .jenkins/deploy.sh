#!/usr/bin/env bash

set -xe

# Deploys on Nexus
# deploy:deploy: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html#setting-up-your-project-to-use-the-build-lifecycle
# $@: the extra parameters to be used in the maven command
main() {
  local extraBuildParams=("$@")

  mvn deploy:deploy \
    --errors \
    --batch-mode \
    --activate-profiles 'DEPLOY' \
    "${extraBuildParams[@]}"
}

main "$@"
