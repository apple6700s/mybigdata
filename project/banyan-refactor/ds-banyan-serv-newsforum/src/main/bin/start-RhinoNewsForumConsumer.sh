#!/usr/bin/env bash
sh delete-checkpoint.sh
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.newsforum.kafka.RhinoNewsForumConsumer >> ../logs/RhinoNewsForumConsumer.log &
clear