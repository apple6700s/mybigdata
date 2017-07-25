#!/usr/bin/env bash
nohup sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR1 $@ > WeiboContentFlushESMR.log &