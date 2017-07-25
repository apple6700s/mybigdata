#!/usr/bin/env bash
sh delete-checkpoint.sh RhinoAsyncDataConsumer
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.asyncdata.kafka.RhinoAsyncDataConsumer > ../logs/RhinoAsyncDataConsumer.log &
clear