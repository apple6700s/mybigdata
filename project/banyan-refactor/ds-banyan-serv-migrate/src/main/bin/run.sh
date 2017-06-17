#!/bin/sh
export HADOOP_USER_NAME=dota
PWD=$(cd $(dirname $0); pwd)
cd $PWD

print_usage ()
{
  echo "Usage: sh run.sh COMMAND"
  echo "where COMMAND is one of the follows:"
  echo "yarnConsume      启动yarn spark streaming etl"
  echo "yarn      启动yarn spark "
  echo "runRetry      启动retrylog处理程序"
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
HEAP_OPTS="-Xmx2000m"

JAR_NAME=`ls |grep jar|grep -v original-|grep dependencies`

CLASSPATH=${CLASSPATH}:${JAVA_HOME}/lib/tools.jar
CLASSPATH=${CLASSPATH}:conf
CLASSPATH=${CLASSPATH}:${JAR_NAME}
for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:${f};
done

params=$@

if [ "$COMMAND" = "yarnConsume" ]; then
    CLASS=com.datastory.banyan.kafka.SparkStreamingYarnLauncher
elif [ "$COMMAND" = "yarn" ]; then
    CLASS=com.datastory.banyan.spark.SparkYarnLauncher
elif [ "$COMMAND" = "runRetry" ]; then
    CLASS=com.datastory.banyan.retry.RetryRunner
else
    CLASS=${COMMAND}
fi

"$JAVA" -Dds.commons.logging.procName=${CLASS} -Dhdp.version=2.4.2.0-258 -Djava.awt.headless=true ${HEAP_OPTS} -classpath "$CLASSPATH" ${CLASS} ${params}
