#!/usr/bin/env bash
nohup sh run.sh yarnConsume com.datastory.banyan.weibo.kafka.RhinoWeiboYarnConsumer > RhinoWeiboYarnConsumer.log &
nohup sh run.sh yarnConsume com.datastory.banyan.weibo.kafka.ToESWeiboConsumer > ToESWeiboConsumer.log &
