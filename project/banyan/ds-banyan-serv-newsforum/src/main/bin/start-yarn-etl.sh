#!/usr/bin/env bash
nohup sh run.sh yarnConsume com.datastory.banyan.newsforum.kafka.RhinoNewsForumYarnConsumer > RhinoNewsForumYarnConsumer.log &
nohup sh run.sh yarnConsume com.datastory.banyan.newsforum.kafka.ToESNewsForumConsumer > ToESNewsForumConsumer.log &
