#!/usr/bin/env bash

# Please distribute this script to all the es cluster nodes.
# This copy action is REQUIRED to be executed while the index is closed.
#
# Currently only work for brand new indexes, because it need to sync the translog id and generation.
# sh copy-from-hdfs.sh ${INDEX} ${HDFS_ROOT_PATH} ${ES_INDEX_PATH}

###### change configs here
#INDEX=ds-es1
INDEX=$1
#HDFS_ROOT_PATH=/tmp/chenzhao/
HDFS_ROOT_PATH=$2

# In ES 2.x, multiple data path actually not work. Only the first path configured in the path.data contatins real lucene data.
#ES_INDEX_PATH=/cloud/data1/sealion/node0/data/rhino_es_cluster/nodes/0/indices
#ES_INDEX_PATH=/data/sealion/data/dev_datatub_es_cluster/nodes/0/indices
ES_INDEX_PATH=$3

###### execute on es node with es user (i.e. sealion in DataStory)

SHARD=$(ls ${ES_INDEX_PATH}/${INDEX} | grep -v _state)

if [ $SHARD"x" != "x" ]; then
    rm -rf ${ES_INDEX_PATH}/${INDEX}/${SHARD}/index
    hadoop dfs -get ${HDFS_ROOT_PATH}/${INDEX}/${SHARD}/index ${ES_INDEX_PATH}/${INDEX}/${SHARD}/
    rm -rf ${ES_INDEX_PATH}/${INDEX}/${SHARD}/translog
    hadoop dfs -get ${HDFS_ROOT_PATH}/${INDEX}/${SHARD}/tanslog/* ${ES_INDEX_PATH}/${INDEX}/${SHARD}/
fi