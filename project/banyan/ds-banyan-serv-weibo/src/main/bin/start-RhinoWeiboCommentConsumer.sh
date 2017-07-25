#!/usr/bin/env bash
NAME=RhinoWeiboCommentConsumer
sh delete-checkpoint.sh ${NAME}
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.weibo.kafka.${NAME} > ../logs/${NAME}.log &