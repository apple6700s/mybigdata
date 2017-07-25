#!/usr/bin/python
# coding: utf-8
__author__ = 'lhfcws'

import json
import os
import time

from pyquery import PyQuery as pq


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


URL = "http://spark-rhino.datatub.com"
YARN_URL = "http://yarn-alps.datatub.com"
# APP_NAMES = ["RhinoWeiboYarnConsumer", "ToESWeiboConsumer", "RhinoNewsForumYarnConsumer", "ToESNewsForumConsumer", "RhinoWechatYarnConsumer", "ToESWechatConsumer"]
APP_NAMES = ["RhinoNewsForumYarnConsumer"]
FAIL_EXECUTOR_STATUS = ["FAILED", "KILLED", "TIMEOUT"]


class APPID(object):
    def __init__(self, app_id, app_name, href):
        self.app_id = app_id
        self.app_name = app_name
        self.href = href

    def __str__(self):
        return self.app_id + ", " + self.app_name + ", " + self.href


def get_yarn_running_apps():
    html = wget(YARN_URL + "/cluster/apps/RUNNING")
    D = pq(html)
    script = D("table script").text()
    script = script.replace("var appsTableData=", "").strip()

    trs = json.loads(script)
    ret = {}
    for tr in trs:
        a = pq(tr[0])
        app_id = a.text()
        href = YARN_URL + "/proxy/" + app_id
        app_name = tr[2]
        app = APPID(app_id, app_name, href)
        if app_name in APP_NAMES:
            ret[app_name] = app
    return ret


def get_running_apps():
    html = wget(URL)

    D = pq(html)
    h4 = D("div.span12 h4")
    ret = {}

    for _h4 in h4:
        h4_text = _h4.text
        if h4_text.strip() == "Running Applications":
            running_apps = D(_h4.getparent()).children("table tbody tr")
            for tr in running_apps:
                tds = tr.getchildren()
                td = D(tds[0])
                app_id = td.text().strip()
                td = D(tds[1])
                a = D(td.html().strip())
                href = a.attr("href")
                # json_url = href + "/metrics/json"
                appname = a.text()
                app = APPID(app_id, appname, href)
                if appname in APP_NAMES:
                    ret[appname] = app
    print(ret)
    return ret


def get_metrics(running_apps):
    ret = {}
    for appname, app in running_apps.items():
        if appname in APP_NAMES:
            try:
                json_url = app.href + "/metrics/json"
                print("Connecting to " + json_url)
                html = wget(json_url)
                ret[appname] = html.strip()
            except Exception as e:
                print(e)

    # print(ret)
    return ret


def parse_log(html):
    D = pq(html)
    pre = D("pre")
    if pre is not None:
        text = pre.text()
        if text is None or len(text.strip()) == 0:
            return None
        else:
            if len(text) > 12800:
                text = "".join(list(text[-12800:]))
            return text

    return None


def get_fail_executors(running_apps):
    ret = {}
    for appname, app in running_apps.items():
        executor_url = app.href + "/executors"
        html = wget(executor_url)

        tmp_storage = "/tmp/" + app.app_id
        err_dict = {}
        try:
            fp = open(tmp_storage, "r")
            err_dict = json.load(fp)
            print(err_dict)
            fp.close()
        except Exception:
            pass

        D = pq(html)
        trs = D("tbody tr")
        logs = []
        for tr in trs:
            exid = tr[0].text
            address = tr[1].text
            fail_cnt = int(tr[6].text)

            if fail_cnt > 0:
                exid1 = app.app_id + "-" + exid
                old_fcnt = 0
                if exid1 in err_dict:
                    old_fcnt = err_dict[exid1]
                if fail_cnt > old_fcnt:
                    # get err log
                    err_dict[exid1] = fail_cnt
                    log_area = D(tr[13])
                    log_area = log_area.find("a")

                    for a in log_area:
                        a = D(a)
                        log_type = a.text()
                        href = a.attr("href")
                        html = wget(href)
                        log_text = parse_log(html)
                        if log_text is not None:
                            logs += [" >>>>> " + exid1 + " " + address + " " + log_type, log_text]

        if len(logs) > 0:
            ret[appname] = (app, "\n".join(logs))
        if len(err_dict) > 0:
            fw = open(tmp_storage, "w")
            json.dump(err_dict, fw)
            fw.flush()
            fw.close()
    return ret


if __name__ == "__main__":
    # running = get_running_apps()
    running = get_yarn_running_apps()
    # res = get_metrics(running)
    res = get_fail_executors(running)

    print(res)
