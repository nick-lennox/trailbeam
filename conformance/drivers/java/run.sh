#!/bin/sh
# Starts the Java driver built by `mvn -f conformance/drivers/java/pom.xml package`. Only this java process
# gets the variant's JAVA_TOOL_OPTIONS (an agent), not Maven. TRAILBEAM_KIT_JAVA_LAUNCH=boot (the spring-boot
# variants) starts the Spring Boot executable jar instead of the plain classpath.
set -e
D=$(cd "$(dirname "$0")" && pwd)
if [ ! -f "$D/target/classpath.txt" ]; then
  echo "conformance/drivers/java is not built: mvn -f java/pom.xml install && mvn -f conformance/drivers/java/pom.xml package" >&2
  exit 2
fi
JAVA=java
[ -n "$JAVA_HOME" ] && JAVA="$JAVA_HOME/bin/java"
if [ "$TRAILBEAM_KIT_JAVA_LAUNCH" = boot ]; then
  set -- "$D"/target/*-boot.jar
  if [ ! -f "$1" ]; then
    echo "conformance/drivers/java: no Spring Boot jar in target/ (rebuild with mvn -f conformance/drivers/java/pom.xml package)" >&2
    exit 2
  fi
  exec "$JAVA" -jar "$1"
fi
exec "$JAVA" -cp "$D/target/classes:$(cat "$D/target/classpath.txt")" trailbeam.kit.Driver
