#!/usr/bin/env bash
ps -ef | grep split-flush-es-cnt | grep -v grep | awk '{print $2}' | xargs kill -9

for i in {3544..3561}
do
	yarn application -kill application_1481941780092_${i}
done