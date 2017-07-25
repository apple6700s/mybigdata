#!/usr/bin/env bash
rm x*.log
rm run_*.log
cd ..
ls x* > tmp
cd -
pwd
while read line
do
    nohup sh split-flush-es-cnt.sh ${line} > run_${line}.log &
done < ../tmp

echo "Total parallism: $(wc -l ../tmp)"
ps -ef | grep split-flush-es-cnt | grep -v grep