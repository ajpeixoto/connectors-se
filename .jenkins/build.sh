#!/usr/bin/env bash

set -xe

# Builds the components with tests, Docker image and spotBugs enabled
# Also generates the Talend components ui spec
# $1: the Jenkinsfile's params.Action
# $2: Execute sonar analysis or not, from jenkinsfile's params.SONAR_ANALYSIS
# $3: Sonar analyzed branch
# $@: the extra parameters to be used in the maven commands
main() (
  jenkinsAction="${1?Missing Jenkins action}"; shift
  sonar="${1?Missing sonar option}"; shift
  branch="${1?Missing branch name}"; shift
  extraBuildParams=("$@")

  mavenPhase='verify'

  # check for format violations. You shall not pass!
  mvn spotless:check

  # Real task
  # ITs profile is added for jdbc
  mvn "${mavenPhase}" \
      --errors \
      --batch-mode \
      --activate-profiles "${jenkinsAction},ITs" \
      "${extraBuildParams[@]}"

  if [[ "${sonar}" == 'true' ]]; then
    declare -a LIST_FILE_ARRAY=( $(find $(pwd) -type f -name 'jacoco.xml') )
    LIST_FILE=$(IFS=, ; echo "${LIST_FILE_ARRAY[*]}")
    # Why sonar plugin is not declared in pom.xml: https://blog.sonarsource.com/we-had-a-dream-mvn-sonarsonar
    mvn sonar:sonar \
        --define 'sonar.host.url=https://sonar-eks.datapwn.com' \
        --define "sonar.login=${SONAR_LOGIN}" \
        --define "sonar.password=${SONAR_PASSWORD}" \
        --define "sonar.branch.name=${branch}" \
        --define "sonar.coverage.jacoco.xmlReportPaths='${LIST_FILE}'" \
        --activate-profiles SONAR \
        "${extraBuildParams[@]}"
  fi
)

main "$@"
