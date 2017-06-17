#!/usr/bin/env bash
nohup python2 table_monitor.py -c es -s todi1:9200 -t 'dt-rhino-newsforum-index, dt-rhino-weibo-comment-index, dt-rhino-weibo-index' >> /tmp/banyan-monitor-db.log &