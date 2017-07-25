#!/usr/bin/env bash
nohup python2 table_monitor.py -c hbase -s todi1 -t 'dt.rhino.weibo.comment, dt.rhino.sys.common.v7, dt.rhino.weibo.user.v2' >> /tmp/banyan-monitor-db.log &