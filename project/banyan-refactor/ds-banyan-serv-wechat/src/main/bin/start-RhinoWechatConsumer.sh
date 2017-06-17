#!/usr/bin/env bash
sh delete-checkpoint.sh
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.wechat.kafka.RhinoWechatConsumer >> ../logs/RhinoWechatConsumer.log &
clear