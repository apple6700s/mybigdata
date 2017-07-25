#!/usr/bin/env bash
cd $(dirname $0)
DIRHOME=$(pwd)
#cd ${DIRHOME}/../../
#PROJ_HOME=$(pwd)
#TARGET=${PROJ_HOME}/ds-banyan-serv-etl/target
#SCRIPT_HOME=${PROJ_HOME}/scripts
# write your shell here

HB_CRONTIME='* * */2 * *'
ES_CRONTIME='*/30 * * * *'
#CRON_CMD="${CRONTIME} cd ${DIRHOME}; sh monitor-db.sh"

LOG='/tmp/banyan-monitor-db.log'
ES_HOST='todi1:9200'
MON_PY='table_monitor.py'

#cd ${SCRIPT_HOME}/python/monitor

# es
nohup python2 table_monitor.py -c es -s todi1:9200 -t 'dt-rhino-newsforum-index, dt-rhino-weibo-comment-index, dt-rhino-weibo-index' >> /tmp/banyan-monitor-db.log &
# hbase
nohup python2 table_monitor.py -c hbase -s todi1 --step 7200 -t 'dt.rhino.weibo.user.v2, dt.rhino.weibo.comment, dt.rhino.sys.common.v7, dt.rhino.weibo.content.v2' >> /tmp/banyan-monitor-db.log &
