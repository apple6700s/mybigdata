#!/usr/bin/env bash
sh delete-checkpoint.sh
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.weibo.kafka.RhinoWeiboConsumer >> ../logs/RhinoWeiboConsumer.log &
clear