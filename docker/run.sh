#!/bin/bash

set -e

export JVM_OPTS="${JVM_OPTS}" # i.e -Xmx1024m

echo "Starting Casspoke"
echo "JVM_OPTS: $JVM_OPTS"

/sbin/dumb-init /usr/bin/java ${JVM_OPTS} -jar /opt/casspoke/casspoke.jar /etc/casspoke/config.yml
