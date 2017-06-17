#!/usr/bin/env bash
FN=$1
shift
NUM=$(wc -l ${FN})

cd ..
while read line
do
    echo "sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentRegionFlushMR ${line}"
    sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentRegionFlushMR ${line}
done < ${FN}