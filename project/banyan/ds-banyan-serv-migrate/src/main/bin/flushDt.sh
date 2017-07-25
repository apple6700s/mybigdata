#!/usr/bin/env bash
HBTABLE='DS_BANYAN_WEIBO_CONTENT_V1,DS_BANYAN_WEIBO_USER'
ESINDEX='ds-banyan-weibo-index-v1'
ESTYPE='weibo,user'
HOST='http://datatub2:9200/'
YML='conf/dt/elasticsearch.yml'

LOG="${ESINDEX}.log"

CMD="nohup sh run.sh 'com.datastory.banyan.migrate1.flush.FlushMR' -hbTable '${HBTABLE}' -index '${ESINDEX}' -type '${ESTYPE}' -host '${HOST}' -yml '${YML}' -conf 'local.data.paths=/tmp/es-data' -conf 'min.free.mb=500' -conf 'mapreduce.reduce.num=1' -conf 'es.index.overwrite=true' > ${LOG} &"
echo $CMD
nohup sh run.sh 'com.datastory.banyan.migrate1.flush.FlushMR' -hbTable "${HBTABLE}" -index "${ESINDEX}"  -type "${ESTYPE}" -host "${HOST}" -yml "${YML}"  -conf 'local.data.paths=/tmp/es-data' -conf 'min.free.mb=500' -conf 'mapreduce.reduce.num=1' -conf 'es.index.overwrite=true' > ${LOG} &
tailf ${LOG}

