#!/usr/bin/env bash
mkdir -p ../logs
#nohup sh run.sh com.datastory.banyan.weibo.process.AdvUserHBaseUpdater -q > ../logs/AdvUserUpdater.out &
nohup sh run.sh com.datastory.banyan.weibo.cli.UserUpdateCli $@ > ../logs/UserUpdate.log &