#!/usr/bin/env bash

set -xe

# Execute sonar analysis
# $1: Sonar analyzed branch
# $@: the extra parameters to be used in the maven commands
main() (
  branch="${1?Missing branch name}"; shift
  extraBuildParams=("$@")

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

)

main "$@"
