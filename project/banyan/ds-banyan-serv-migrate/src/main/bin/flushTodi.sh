#!/usr/bin/env bash
HBTABLE=$1
ESINDEX=$2
ESTYPE=$3
HOST='http://todi1:9200/'
# 不要加入 /cloud/data1
CONF='local.data.paths=/cloud/data2/ds-es-flush,/cloud/data3/ds-es-flush,/cloud/data4/ds-es-flush'
ESYML='conf/todi/elasticsearch.yml'

LOG="${ESINDEX}.log"

CMD="nohup sh run.sh 'com.datastory.banyan.migrate1.flush.FlushMR' -hbTable '${HBTABLE}' -index '${ESINDEX}' -type '${ESTYPE}' -host '${HOST}' -yml '${ESYML}' -conf 'local.data.paths=/cloud/data2/ds-es-flush,/cloud/data3/ds-es-flush,/cloud/data4/ds-es-flush' -conf 'min.free.mb=512000' > ${LOG} &"
echo $CMD
nohup sh run.sh 'com.datastory.banyan.migrate1.flush.FlushMR' -hbTable "${HBTABLE}" -index "${ESINDEX}" -type "${ESTYPE}" -host "${HOST}"  -yml "${ESYML}" -conf 'local.data.paths=/cloud/data2/ds-es-flush,/cloud/data3/ds-es-flush,/cloud/data4/ds-es-flush' -conf 'min.free.mb=512000' > ${LOG} &
tailf ${LOG}

