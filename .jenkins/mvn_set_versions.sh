#!/usr/bin/env bash
#
#  Copyright (C) 2006-2023 Talend Inc. - www.talend.com
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

set -xe

# This script allow to change the version of current maven project
# You can provide extra parameters to tune maven
# $1: final_version
# $2: extra_mvn_parameters
main() {
  _FINAL_VERSION="${1?Missing requested final version}"; shift
  _EXTRA_MVN_PARAMETERS=("$@")

  printf '##############################################\n'
  printf "Edit version for dev branches, new version is %s\n" "${_FINAL_VERSION}"
  printf '##############################################\n'

  mvn versions:set --define newVersion="${_FINAL_VERSION}"\
                   ${_EXTRA_MVN_PARAMETERS[*]}
}

main "$@"
