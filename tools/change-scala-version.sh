#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -e

BASEDIR=$(dirname $0)/..
TO_VERSION=$1
TO_BINARY_VERSION=$2

CHILL_VERSION_211=0.7.4
CHILL_VERSION_212=0.7.6

VALID_BINARY_VERSIONS=( 2.11 2.12 )
check_scala_version() {
  for i in ${VALID_BINARY_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_BINARY_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_BINARY_VERSION"

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

sed_i '1,/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</s/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</<scala.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"

sed_i '1,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_BINARY_VERSION'</' \
  "$BASEDIR/pom.xml"

if [ $TO_BINARY_VERSION = "2.12" ]; then
    # also need to update our chill dependency because there is no 2.12 release
    # for chill 0.7.4
    sed_i '1,/<chill\.version>[0-9]*\.[0-9]*\.[0-9]*</s/<chill\.version>[0-9]*\.[0-9]*\.[0-9]*</<chill.version>'$CHILL_VERSION_212'</' \
      "$BASEDIR/pom.xml"
elif [ $TO_BINARY_VERSION = "2.11" ]; then
    sed_i '1,/<chill\.version>[1-9]*\.[0-9]*\.[0-9]*</s/<chill\.version>[0-9]*\.[0-9]*\.[0-9]*</<chill.version>'$CHILL_VERSION_211'</' \
      "$BASEDIR/pom.xml"
fi
