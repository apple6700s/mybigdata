#!/bin/sh
print_usage ()
{
  echo "Usage: sh run.sh COMMAND"
  echo "where COMMAND is one of the follows:"
  exit 1
}

if [ $# = 0 ] || [ $1 = "help" ]; then
  print_usage
fi

COMMAND=$1
shift

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi


JAVA=${JAVA_HOME}/bin/java
HEAP_OPTS="-Xmx10000m -XX:MaxPermSize=256m -XX:PermSize=128m"

CLASSPATH=${CLASSPATH}:conf
CLASSPATH=${CLASSPATH}:`ls |grep jar|grep ds-dbamp-serv-sync-*-SNAPSHOT.jar`
for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:${f};
done

params=$@

if [ "$COMMAND" = "ProductJobCli" ]; then
    CLASS=com.dt.mig.gen.cli.ProductJobCli
else
    CLASS=${COMMAND}
fi

"$JAVA" -Djava.io.tmpdir=/var/spark/tmp -Djava.awt.headless=true ${HEAP_OPTS} -classpath "$CLASSPATH" ${CLASS} ${params}