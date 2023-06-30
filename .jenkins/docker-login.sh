#!/usr/bin/env bash

set -xe

# Logs in to the Talend Docker registry
# $1: The Talend Docker registry host, take it from the Jenkins global variables
# $2: The login for artifactory
# $3: The password for artifactory
main() (
  artifactory_registryhost="${1?Missing artifactory registry host}"
  artifactory_login="${2?Missing artifactory login environment}"
  artifactory_password="${3?Missing artifactory password environment}"


  MAX_RETRIES=5
  SLEEP_RETRIES_S=1 # in seconds
  retries=0
  connection_success=false

  while [[ $retries -lt $MAX_RETRIES ]]; do
    printf "Login to the Docker registry attempt #%s.\n", "$((retries+1))"

    if docker version; then
      connection_success=true
      break
    fi

    retries=$((retries+1))
    sleep $SLEEP_RETRIES_S
  done

  docker login "${artifactory_registryhost}" \
                   --username "${artifactory_login}" \
                   --password-stdin <<< "${artifactory_password}"

  if ! $connection_success; then
    printf "Failed to login to the Docker registry after %s attempts.\n", $MAX_RETRIES
    exit 1
  fi
)

main "$@"
