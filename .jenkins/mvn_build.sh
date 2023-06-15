#!/usr/bin/env bash

set -xe

# Builds the components with tests
# Also generates the Talend components ui spec
# $@: the extra parameters to be used in the maven commands
main() (
  extraBuildParams=("$@")

  # check for format violations. You shall not pass!
  mvn spotless:check

  mvn clean install \
      --errors \
      --batch-mode \
      --activate-profiles 'STANDARD, ITs'\
      "${extraBuildParams[@]}"

  mvn -Dbuildinfo.reproducible=true artifact:buildinfo
)

main "$@"
