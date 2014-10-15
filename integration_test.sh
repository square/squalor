#!/bin/bash
#
# Copyright 2014 Square Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Verify that Docker is installed.
if [[ ! $(type -P docker) ]]; then
  echo "Docker executable not found!"
  echo "Installation instructions at https://docs.docker.com/installation/"
  exit 1
fi

# Verify docker is reachable.
if ! docker ps >/dev/null 2>&1; then
  echo "Docker is not reachable. Did you follow installation instructions?"
  exit 1
fi

function stop() {
  local cids=$@
  if [ -n "${cids}" ]; then
    docker kill ${cids} >/dev/null
    docker rm ${cids} >/dev/null
  fi
}

# Stop any destroy any previously running squalor-test containers.
stop $(docker ps -a | grep squalor-test | awk '{print $1}')

export MYSQL_HOST=$(echo ${DOCKER_HOST:-"tcp://127.0.0.1:0"} | sed -E 's,tcp://(.*):.*,\1:3306,')
export MYSQL_PASSWORD=password

# Start our mysql container.
cid=$(docker run -d --name squalor-test -e MYSQL_ROOT_PASSWORD=${MYSQL_PASSWORD} -p 3306:3306 mysql:5.6)
trap "stop ${cid}" 0

# Wait for mysql to be ready for connections.
ok=0
for i in {1..20}; do
  if docker logs ${cid} 2>&1 | grep -q 'ready for connections'; then
    ok=1
    break
  fi
  sleep 1
done
if [ ${ok} != 1 ]; then
  echo "Failed to start"
  docker logs ${cid}
  exit 1
fi

# Run the tests.
go test .
