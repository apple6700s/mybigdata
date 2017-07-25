#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Modifier: Lhfcws Wu
Author: Li Zhuohui
Email: zhuohui@hudongpai.com
Date: 16/11/7
"""

import json
import requests
import time
import socket
import commands
import re
import sys
import datetime
import smtplib
from email.mime.text import MIMEText

from banyan_common import *

SPLIT_CHAR = ","
STEP = 30
RUNTIME = datetime.datetime.now()


def bytes_to_gigabyte(size):
    return round(float(size) / 1024 / 1024 / 1024, 2)


def get_es_data(servers, index_list):
    """
    :param servers: es node list
    :param index_list: es index list
    :return:
    """

    def make_stat_request(index_name, tries=3):
        for server in servers:
            server = server.strip()
            url = "http://{server}/{index}/_stats".format(server=server, index=index_name)
            while tries > 0:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code != 200:
                        time.sleep(1)
                        continue

                    return json.loads(response.content)["_all"]
                except:
                    time.sleep(1)
                finally:
                    tries -= 1

    data = {}
    for index in index_list:
        index = index.strip()
        index_stat = make_stat_request(index)
        if index_stat:
            data[index] = {
                "es.docs.num": index_stat["primaries"]["docs"]["count"],
                "es.total.docs.num": index_stat["total"]["docs"]["count"] + index_stat["total"]["docs"]["deleted"],
                "es.gb.size": bytes_to_gigabyte(index_stat["primaries"]["store"]["size_in_bytes"]),
                "es.total.gb.size": bytes_to_gigabyte(index_stat["total"]["store"]["size_in_bytes"])
            }

    return data


def get_hbase_data(servers, table_list):
    def parse_count(output):
        """
         从phoenix查询结果里面解析出count
        :param servers: zookeeper lists
        :param table_list: hbase table list
        :return:
        """
        for line in output.split("\n"):
            pattern = re.compile("COUNT\(1\)[^0-9]*([0-9]+)")
            match = re.search(pattern, line)
            if match:
                return match.group(1)

    def output_sql(filename, sql):
        with open(filename, "w+") as f:
            f.write(sql)
            f.write("\n")

    def make_query(table, tries=3):
        for server in servers:
            server = server.strip()
            while tries > 0:
                sql = "!outputformat vertical\nselect count(1) from \"{table}\"".format(table=table)
                filename = "/tmp/hbase_query_temp_" + table
                output_sql(filename, sql)
                cmd = "/usr/hdp/current/phoenix-client/bin/sqlline.py {server}:/hbase-unsecure {sql_file}".format(
                    server=server, sql_file=filename
                )
                (status, output) = commands.getstatusoutput(cmd)
                if status != 0:
                    print "[ERROR] " + output
                else:
                    return parse_count(output)

                tries -= 1
        return None

    data = {}
    for table in table_list:
        table = table.strip()
        count = make_query(table)
        if count is not None:
            data[table] = {
                "hbase.count.num": count
            }
    return data


def push_to_open_falcon(data):
    falcon_data = []
    endpoint = socket.gethostname()
    ts = int(time.time())
    for name, metrics in data.items():
        for metric, value in metrics.items():
            falcon_data.append({
                "endpoint": endpoint,
                "metric": metric,
                "timestamp": ts,
                "step": STEP,
                "value": value,
                "counterType": "GAUGE",
                "tags": "group=ds,project=banyan,table=" + name,
            })

    tries = 3
    while tries > 0:
        try:
            response = requests.post("http://localhost:1988/v1/push", data=json.dumps(falcon_data))
            if not response.ok:
                continue
            return
        except:
            continue
        finally:
            tries -= 1


def usage():
    print "Usage: python " + sys.argv[
        0] + " [-s|--servers] server1,server2 [-t|--tables] table1,table2 [-c|--component] [hbase|es]"
    print "Example: python " + sys.argv[0] + " -s todi1:2181 -t dt.rhino.sys.common -c hbase"


def send_email(data, component):
    """
    定期发送邮件，10, 12, 14, 15, 16, 18, 19, 21点发当前结果的邮件
    :param data:
    :param component: 组件名称, hbase|es
    :return:
    """
    now = RUNTIME
    can_send = now.hour in [10, 12, 14, 15, 16, 18, 19, 21]
    can_send &= now.minute < 1
    # can_send = True
    if can_send:
        content = ""
        keys = data.keys()
        keys.sort()
        for name in keys:
            metrics = data[name]
            _keys = metrics.keys()
            _keys.sort()
            content += name + "\n"
            for metric in _keys:
                value = metrics[metric]
                content += " -> " + metric + " = " + str(value) + "\n"
            content += "\n"

        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        server = smtplib.SMTP('smtp.exmail.qq.com.')
        server.starttls()
        msg = MIMEText(content)
        msg["Subject"] = "[监控] ({current}) BANYAN {component}库表监控".format(current=current_time, component=component)
        msg["From"] = EMAIL_USER
        msg["To"] = ",".join(TO_ADDRESSES)
        print server.login(FROM_ADDRESS, EMAIL_PASSWORD)
        print ",".join(TO_ADDRESSES)
        print server.sendmail(FROM_ADDRESS, TO_ADDRESSES, msg.as_string())
        server.quit()


if __name__ == "__main__":
    global RUNTIME
    RUNTIME = datetime.datetime.now()
    idx = 1
    servers = []
    tables = []
    component = "hbase"
    while idx < len(sys.argv):
        if sys.argv[idx] == "--servers" or sys.argv[idx] == "-s":
            idx += 1
            servers = sys.argv[idx].split(SPLIT_CHAR)
        elif sys.argv[idx] == "--tables" or sys.argv[idx] == "-t":
            idx += 1
            tables = sys.argv[idx].split(SPLIT_CHAR)
        elif sys.argv[idx] == "--component" or sys.argv[idx] == "-c":
            idx += 1
            component = sys.argv[idx]
        elif sys.argv[idx] == "--step":
            idx += 1
            STEP = sys.argv[idx]

        idx += 1

    if len(servers) == 0 or len(tables) == 0:
        usage()
        sys.exit(1)

    data = {}
    if component == "es":
        data = get_es_data(servers, tables)
    elif component == "hbase":
        data = get_hbase_data(servers, tables)
    else:
        usage()
        sys.exit(1)

    print(data)
    push_to_open_falcon(data)
    send_email(data, component)
