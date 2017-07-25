#!/usr/bin/env bash
hadoop dfs -rmr /tmp/banyan/checkpoint/RhinoNewsForumDirectConsumerBushu
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.newsforum.kafka.RhinoNewsForumDirectConsumerBushu >> ../logs/RhinoNewsForumDirectConsumerBushu.log &
clear