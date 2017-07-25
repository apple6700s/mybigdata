#!/usr/bin/env bash
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.weibo.kafka.ToESWeiboConsumer > ../logs/ToESWeiboConsumer.out &