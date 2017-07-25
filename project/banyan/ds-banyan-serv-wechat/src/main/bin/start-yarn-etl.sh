#!/usr/bin/env bash
nohup sh run.sh yarnConsume com.datastory.banyan.wechat.kafka.RhinoWechatYarnConsumer > RhinoWechatYarnConsumer.log &
nohup sh run.sh yarnConsume com.datastory.banyan.wechat.kafka.ToESWechatConsumer > ToESWechatConsumer.log &
