#!/usr/bin/env bash
nohup sh run.sh com.datastory.banyan.asyncdata.retry.AsyncDataRetryRunner -q > ../RetryRunner.log &
