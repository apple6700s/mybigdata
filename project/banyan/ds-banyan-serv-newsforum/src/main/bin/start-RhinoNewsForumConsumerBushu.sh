#!/usr/bin/env bash
hadoop dfs -rmr /tmp/banyan/checkpoint/RhinoNewsForumConsumerBushu
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.newsforum.kafka.RhinoNewsForumConsumerBushu >> ../logs/RhinoNewsForumConsumerBushu.log &
clear