#!/usr/bin/env bash
hadoop dfs -rmr /tmp/banyan/checkpoint/RhinoNewsForumDirectConsumer
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.newsforum.kafka.RhinoNewsForumDirectConsumer >> ../logs/RhinoNewsForumDirectConsumer.log &
clear