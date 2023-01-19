#!/usr/bin/env bash

set -xe

# Builds the components with tests
# Also generates the Talend components ui spec
# $1: Execute sonar analysis or not
# $2: Sonar analyzed branch
# $@: the extra parameters to be used in the maven commands
main() (
  sonar="${1?Missing sonar option}"; shift
  branch="${1?Missing branch name}"; shift
  extraBuildParams=("$@")

  # check for format violations. You shall not pass!
  mvn spotless:check

  # Maven phases:
  # >>> validate - validate the project is correct and all necessary information is available
  # >>> compile - compile the source code of the project
  # >>> test - test the compiled source code using a suitable unit testing framework
  # >>> package - take the compiled code and package it in its distributable format, such as a JAR
  # >>> verify - run any checks on results of integration tests to ensure quality criteria are met
  # >>> install - install the package into the local repository
  # xxx deploy - copies the final package to the remote repository

  mvn clean install \
      --errors \
      --batch-mode \
      --activate-profiles 'STANDARD, ITs'\
      "${extraBuildParams[@]}"

  if [[ "${sonar}" == 'true' ]]; then
    declare -a LIST_FILE_ARRAY=( $(find $(pwd) -type f -name 'jacoco.xml') )
    LIST_FILE=$(IFS=, ; echo "${LIST_FILE_ARRAY[*]}")
    # Why sonar plugin is not declared in pom.xml: https://blog.sonarsource.com/we-had-a-dream-mvn-sonarsonar
    # TODO https://jira.talendforge.org/browse/TDI-48980 (CI: Reactivate Sonar cache)
    mvn sonar:sonar \
        --define 'sonar.host.url=https://sonar-eks.datapwn.com' \
        --define "sonar.login=${SONAR_LOGIN}" \
        --define "sonar.password=${SONAR_PASSWORD}" \
        --define "sonar.branch.name=${branch}" \
        --define "sonar.coverage.jacoco.xmlReportPaths='${LIST_FILE}'" \
        --define "sonar.analysisCache.enabled=false" \
        --activate-profiles SONAR \
        "${extraBuildParams[@]}"
  fi
)

main "$@"
