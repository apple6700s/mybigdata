#!/usr/bin/python2
# coding: utf-8
__author__ = 'lhfcws'

import os
import time
import sys

BROKERS = "alps18:6667,alps3:6667,alps35:6667,alps51:6667,alps8:6667"
ZKCONN = "alps34:2181"


# pass_test
def ts2str(timeStamp):
    timeStamp = int(timeStamp)
    timeStamp /= 1000
    timeArray = time.localtime(timeStamp)
    return time.strftime("%Y%m%d%H%M%S", timeArray)


# pass_test
def str2ts(s):
    timeArray = time.strptime(s, "%Y%m%d%H%M%S")
    timeStamp = int(time.mktime(timeArray))
    return int(timeStamp * 1000)


def get_offset(topic, st_time):
    ts = str2ts(st_time)
    cmd = "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list " + \
          BROKERS + " --topic " + topic + " --time " + str(ts)
    print(cmd)
    fp = os.popen(cmd)

    partitions = {}
    for line in fp.readlines():
        line = line.strip()
        ls = [i.strip() for i in line.split(":")]
        if ls[2] == "":
            continue
        partitions[ls[1]] = ls[2]
    return partitions


def set_offset(group, topic, partitions):
    print(group)
    print(topic)
    print(partitions)
    fn = "/tmp/" + group + topic
    fw = open(fn, "w")

    for partition, offset in partitions.items():
        fw.write("set /consumers/" + group + "/offsets/" + topic + "/" + partition + " " + offset + "\n")
    fw.flush()
    fw.close()

    cmd = "/usr/hdp/current/zookeeper-client/bin/zkCli.sh -server alps17:2181 "
    cmd = "cat " + fn + " | " + cmd
    os.system(cmd)
    print(cmd + " done , remove file " + fn)
    os.remove(fn)


if __name__ == "__main__":
    group, topic, tstr = sys.argv[1], sys.argv[2], sys.argv[3]
    partitions = get_offset(topic, tstr)
    if partitions is not None and len(partitions) > 0:
        set_offset(group, topic, partitions)
    else:
        print("[ERROR] partition is None, sth is wrong.")



