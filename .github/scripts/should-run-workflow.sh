#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

write_output() {
  echo "run_workflow=$1" >> "${GITHUB_OUTPUT}"
}

if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
  write_output true
  exit 0
fi

if [[ "${FULL_CI_LABEL:-false}" == "true" ]]; then
  write_output true
  exit 0
fi

base_sha="${BASE_SHA:-}"
if [[ -z "${base_sha}" ]]; then
  write_output true
  exit 0
fi

ignored_patterns=()
while IFS= read -r pattern; do
  if [[ -n "${pattern}" ]]; then
    ignored_patterns+=("${pattern}")
  fi
done <<< "${IGNORED_PATHS:-}"

is_ignored() {
  local file="$1"
  local pattern
  for pattern in "${ignored_patterns[@]}"; do
    if [[ "${file}" == ${pattern} ]]; then
      return 0
    fi
  done
  return 1
}

changed_files=()
read_changed_files() {
  local file
  while IFS= read -r file; do
    if [[ -n "${file}" ]]; then
      changed_files+=("${file}")
    fi
  done
}

if [[ -n "${CHANGED_FILES_FILE:-}" ]]; then
  read_changed_files < "${CHANGED_FILES_FILE}"
else
  git fetch --no-tags --depth=1 origin "${base_sha}"
  read_changed_files < <(git diff --name-only "${base_sha}" HEAD)
fi

if [[ ${#changed_files[@]} -eq 0 ]]; then
  write_output true
  exit 0
fi

for file in "${changed_files[@]}"; do
  if ! is_ignored "${file}"; then
    write_output true
    exit 0
  fi
done

write_output false
