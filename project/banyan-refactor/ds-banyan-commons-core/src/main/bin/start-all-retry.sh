#!/usr/bin/env bash
jps | grep Retry | awk '{print $1}' | xargs -i kill -9 {}

projs=(newsforum weibo wechat)

for i in ${projs[@]}
do
    cd $i/target
    sh start-retry.sh
    cd -
done

ps -ef | grep RetryRunner | grep -v grep | grep java
