#!/usr/bin/env bash
nohup sh run.sh yarnConsume com.datastory.banyan.weibo.kafka.RhinoWeiboCommentYarnConsumer > RhinoWeiboCommentYarnConsumer.log &
nohup sh run.sh yarnConsume com.datastory.banyan.weibo.kafka.RhinoWeiboTrendYarnConsumer > RhinoWeiboTrendYarnConsumer.log &
