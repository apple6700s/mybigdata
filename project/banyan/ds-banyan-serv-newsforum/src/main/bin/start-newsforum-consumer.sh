#!/usr/bin/env bash
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.newsforum.kafka.ToESNewsForumConsumer > ../logs/ToESewsForumConsumer.out &