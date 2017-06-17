#!/usr/bin/python
# coding: utf-8
__author__ = 'lhfcws'

import atexit
import datetime
import os
import re
import sys
import time
import multiprocessing

sys.stdout = sys.stderr

P_TAG = "<[^>]+>"
SPARK_WEB = "http://spark-rhino.datatub.com"
STATUS = ["NORMAL", "PILEUP", "STUCK"]


class ProcessWrapper(object):
    def __init__(self):
        self.process = None

    def get_process(self):
        return self.process

    def set_process(self, p):
        self.process = p


pw = ProcessWrapper()


def exit_handler():
    sys.stdout.flush()
    sys.stderr.flush()
    p = pw.get_process()
    if p is not None:
        p.terminate()


atexit.register(exit_handler)


def wget(url):
    output = str(time.time()) + "." + str(os.getpid()) + ".html"
    cmd = "wget --http-user=admin --http-password=adminamkt  -O " + output + " " + url
    print(cmd)
    os.system(cmd)
    fp = open(output, "r")
    html = fp.read()
    fp.close()
    os.remove(output)
    return html


def spark_receiver_stuck(name):
    # get app url
    print("==================== check spark_receiver_stuck")
    html = wget(SPARK_WEB)
    p = re.compile("<a[^>]+>%s</a>" % name)
    matcher = p.search(html)
    if matcher is None:
        return False

    app_url = None
    while True:
        g = matcher.group()
        if g is None:
            return False
        if g.count("history"):
            continue

        print(g)
        g = g.replace(name, "")
        g = g.replace("<a href=\"", "")
        g = g.replace("</a>", "")
        g = g.replace("\">", "")
        app_url = g.strip()
        break

    if app_url is None:
        return False

    # get latest batch time
    app_url += "/streaming"
    print(app_url)
    html = wget(app_url)
    p = re.compile("batch\?id=[0-9]+")
    phase = 0
    in_queue = 0
    diff = 0
    for line in html.split("\n"):
        if line.count("Active Batches") > 0:
            phase = 1
        elif line.count("Completed Batches") > 0:
            phase = 2

        if phase == 0:
            continue

        matcher = p.search(line)
        if matcher is None:
            continue
        if phase == 1:
            in_queue += 1
        elif phase == 2:
            g = matcher.group()
            print(g)
            timestamp = int(g.replace("batch?id=", ""))
            timestamp = int(timestamp / 1000)
            dt = datetime.datetime.fromtimestamp(timestamp=timestamp)
            now = datetime.datetime.now()
            print(now)
            print(dt)
            delta = now - dt
            diff = (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 10 ** 6) / 10 ** 6
            print(diff)
            break

    if in_queue == 0 and diff > 10 * 60:
        status = STATUS[2]
    elif in_queue > 0 and diff > 2 * 3600:
        status = STATUS[1]
    else:
        status = STATUS[0]
    print(status)
    print("====================================")
    return status


def exist_process(pid):
    cmd = "ps -ef | grep -v grep | grep -v daemon | grep ' %s '" % pid
    fp = os.popen(cmd)
    res = fp.read()
    print("PROCESS: " + res)

    if res is None or len(res.strip()) == 0:
        return False
    else:
        return True


def start_process(args, name):
    print("==================== restart process")
    cmd = "nohup %s >> %s.log &" % (args, name)
    os.system(cmd)
    print(cmd)
    print("===================  restart process end")
    sys.stdout.flush()
    sys.stderr.flush()
    # exit , new java process will launch a new python daemon process by using new args
    exit(0)


def monitor(name, pid, args):
    if not exist_process(pid):
        print("===== process " + pid + " is dead")
        start_process(args, name)
    else:
        status = spark_receiver_stuck(name)
        print("===== process status: " + status)
        if status == STATUS[1]:
            os.system("kill " + pid)
            start_process(args, name)
        elif status == STATUS[2]:
            os.system("kill -9 " + pid)
            start_process(args, name)


def run():
    print("$$$$$$$$$$$$$$$$$$$$$$$ NEW DAEMON")
    name = sys.argv[1]
    pid = sys.argv[2]
    args_file = sys.argv[3]
    print(sys.argv)

    fp = open(args_file, "r")
    args = fp.read()
    args = args.strip()
    fp.close()
    print(args)

    os.system("rm " + args_file)

    def loop():
        try:
            print("###################### " + time.ctime())
            monitor(name, pid, args)
            print("------------##########")
        except Exception as e:
            print(e)

    while True:
        time.sleep(60)
        process = multiprocessing.Process(target=loop)

        process.start()
        pw.set_process(process)

        process.join(60)
        process.terminate()
        pw.set_process(None)


if __name__ == "__main__":
    # res = spark_receiver_stuck("RhinoWechatConsumer")
    # print(res)
    run()
