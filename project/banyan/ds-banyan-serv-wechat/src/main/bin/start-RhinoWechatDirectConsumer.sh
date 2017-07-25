#!/usr/bin/env bash
hadoop dfs -rmr /tmp/banyan/checkpoint/RhinoWechatDirectConsumer
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.wechat.kafka.RhinoWechatDirectConsumer >> ../logs/RhinoWechatDirectConsumer.log &
clear