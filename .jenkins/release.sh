#!/usr/bin/env bash

set -xe

# Releases the components
# $1: the Jenkinsfile's params.Action
# $2: the version being released (semver, extracted from pom)
# $@: the extra parameters to be used in the maven commands
main() (


  ## Get release parameters
  jenkinsAction="${1?Missing Jenkins action}"; shift
  releaseVersion="${1?Missing release version}"; shift
  extraBuildParams=("$@")

  ## Set project version to new release value
  setMavenVersion "${releaseVersion}"
  setMavenProperty 'common.version' "${releaseVersion}"

  ## Build and deploy the project
  mvn deploy \
    --errors \
    --batch-mode \
    --activate-profiles "${jenkinsAction}" \
    "${extraBuildParams[@]}"

  ## Publish edited files
  git add --update
  git commit --message "[jenkins-release] Release ${releaseVersion}"

  ## Get version details and create the next version value
  majorVersion="$(cut --delimiter '.' --fields 1 <<< "${releaseVersion}")"
  minorVersion="$(cut --delimiter '.' --fields 2 <<< "${releaseVersion}")"
  patchVersion="$(cut --delimiter '.' --fields 3 <<< "${releaseVersion}")"
  postReleaseVersion="${majorVersion}.${minorVersion}.$((patchVersion + 1))-SNAPSHOT"

  ## Publish new released tag to github
  tag="release/${releaseVersion}"
  git tag "${tag}"

  ## Set the next version in POMs
  setMavenVersion "${postReleaseVersion}"

  ## Commit the next version
  git add --update
  git commit --message "[jenkins-release] Prepare for next development iteration ${postReleaseVersion}"

  ## Push modification to git
  git push origin "${tag}"
  git push origin "$(git rev-parse --abbrev-ref HEAD)"
)

setMavenVersion() (
  version="$1"
  mvn 'versions:set' \
    --batch-mode \
    --define "newVersion=${version}"
)

setMavenProperty() (
  propertyName="$1"
  propertyValue="$2"
  mvn 'versions:set-property' \
    --batch-mode \
    --define "property=${propertyName}" \
    --define "newVersion=${propertyValue}"
)

main "$@"
