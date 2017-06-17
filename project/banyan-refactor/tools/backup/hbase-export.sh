#!/usr/bin/env bash

# run in todi/alps61-63 for banyan

TABLE=$1
shift
OUTPUTDIR=/tmp/${TABLE}


hadoop dfs -ls ${OUTPUTDIR}.0
DEL=0
WRT=1
RET0=$?
if [ $RET0 == 0 ]
then
	DEL=0
	WRT=1
else
	hadoop dfs -ls ${OUTPUTDIR}.1
	RET1=$?
	if [ $RET1 == 0 ]
	then
		DEL=1
		WRT=0
	fi
fi

hbase org.apache.hadoop.hbase.mapreduce.Export \
-Dmapreduce.job.running.map.limit=600 \
-Dmapreduce.output.fileoutputformat.compress=true \
-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
-Dmapreduce.output.fileoutputformat.compress.type=BLOCK \
-Dhbase.client.scanner.caching=100 \
-Dmapreduce.map.speculative=false \
-Dmapreduce.reduce.speculative=false \
$TABLE ${OUTPUTDIR}.${WRT}

RET=$?
if [ $RET == 0 ]
then
	hadoop dfs -rmr ${OUTPUTDIR}.${DEL}
fi