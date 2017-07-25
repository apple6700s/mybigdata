#!/usr/bin/env bash

NAME=RhinoAsyncDataConsumer

if [ $# > 1 ]; then
        NAME=$1
fi

hadoop dfs -rmr /tmp/banyan/checkpoint/${NAME}