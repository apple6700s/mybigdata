#!/usr/bin/env bash
nohup sh run.sh com.datastory.banyan.wechat.flush_es.WechatContentFlushESMR $@ > cnt.log &
