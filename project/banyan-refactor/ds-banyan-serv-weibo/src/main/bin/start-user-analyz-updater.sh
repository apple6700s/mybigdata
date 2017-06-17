#!/usr/bin/env bash
mkdir -p ../logs
nohup sh run.sh com.datastory.banyan.weibo.analyz.WbUserAnalyzer > ../logs/WbUserAnalyzer.log &