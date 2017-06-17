#!/usr/bin/env bash
nohup sh run.sh com.datastory.banyan.monitor.daemon.Daemonizer $@ >> ../daemon.$1.log &
clear
exit 0