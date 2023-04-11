#!/usr/bin/env bash

set -xe

# Run checkstyle
# $@: the extra parameters to be used in the maven commands
main() (

  extraBuildParams=("$@")

  mvn checkstyle:checkstyle-aggregate "${extraBuildParams[@]}"

)

main "$@"
