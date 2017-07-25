#!/usr/bin/env bash
sh delete-checkpoint.sh RhinoRealtimeConsumer
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.asyncdata.kafka.RhinoRealtimeConsumer > ../logs/RhinoRealtimeConsumer.log &
clear