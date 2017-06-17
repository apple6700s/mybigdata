#!/usr/bin/env bash

cd $(dirname $0)
DIRHOME=$(pwd)

cd ${DIRHOME}/../
rm -rf target
ln -s ds-banyan-serv-etl/target target

cd ${DIRHOME}

