#!/usr/bin/env bash
hadoop dfs -rmr /tmp/banyan/checkpoint/RhinoWeiboDirectConsumerBushu
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.weibo.kafka.RhinoWeiboDirectConsumerBushu >> ../logs/RhinoWeiboDirectConsumerBushu.log &
clear