#!/usr/bin/env bash

NAME=RhinoNewsForumDirectConsumer

if [ $# -gt 1 ]; then
        NAME=$1
fi


ps -ef | grep -v grep | grep daemon | grep $NAME | awk '{print $2}' | xargs -i kill {}
ps -ef | grep -v grep | grep $NAME | awk '{print $2}' | xargs -i kill {}

#sleep 15
#sh run.sh com.datastory.banyan.monitor.daemon.ReceiverKill $NAME

ps -ef | grep -v grep | grep daemon | grep $NAME
jps | grep $NAME