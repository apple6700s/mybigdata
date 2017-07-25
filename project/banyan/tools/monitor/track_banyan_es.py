#!/usr/bin/python
# coding: utf-8
__author__ = 'lhfcws'

import datetime
import json
import os

URL = "http://es-rhino.datatub.com"
# URL = "http://de1:9200"
MSG_DAY = " 日量跟踪 "
MSG_MONTH = " 月量跟踪 "
MSG_HOUR = " 小时量跟踪 "
WB_INDEX = "dt-rhino-weibo-index"
WX_INDEX = "ds-banyan-wechat-index"
NF_INDEX = "ds-banyan-newsforum-index"
NF_INDEX1 = "dt-rhino-newsforum-index"
HOUR_FORMAT = "%Y%m%d%H"
DAY_FORMAT = "%Y%m%d"
MONTH_FORMAT = "%Y%m"

wb_month_query = "curl -u admin:adminamkt -POST '%s/%s/weibo/_search' -d '{ " % (URL, WB_INDEX) + \
                 '''
                   "size": 0,
                   "query": {
                     "bool": {
                       "must": [
                         {
                           "range": {
                             "post_time_date": {
                               "from": "%s01",
                               "to": "%s31"
                             }
                           }
                         }
                       ]
                     }
                   }
                 }'
                 '''

wb_recent_days_query = "curl -u admin:adminamkt -POST '%s/%s/weibo/_search' -d '{ " % (URL, WB_INDEX) + \
                       '''
                         "size": 0,
                         "query": {
                           "bool": {
                             "must": [
                               {
                                 "range": {
                                   "post_time_date": {
                                     "from": "%s",
                                     "to": "%s"
                                   }
                                 }
                               }
                             ]
                           }
                         },
                         "aggregations": {
                           "post1": {
                             "terms": {
                               "field": "post_time_date",
                               "order": {
                                 "_term": "asc"
                               },
                               "size": 31
                             }
                           }
                         }
                       }'
                       '''

nf_month_query = "curl -u admin:adminamkt -POST '%s/%s/_search' -d '{ " % (URL, NF_INDEX) + \
                 '''
                   "size": 0,
                   "query": {
                     "bool": {
                       "must": [
                         {
                           "range": {
                             "publish_date_date": {
                               "from": "%s01",
                               "to": "%s31"
                             }
                           }
                         }
                       ]
                     }
                   }
                 }'
                 '''

nf_recent_days_query = "curl -u admin:adminamkt -POST '%s/%s/_search' -d '{ " % (URL, NF_INDEX) + \
                       '''
                         "size": 0,
                         "query": {
                           "bool": {
                             "must": [
                               {
                                 "range": {
                                   "publish_date_date": {
                                     "from": "%s",
                                     "to": "%s"
                                   }
                                 }
                               }
                             ]
                           }
                         },
                         "aggregations": {
                           "post1": {
                             "terms": {
                               "field": "publish_date_date",
                               "order": {
                                 "_term": "desc"
                               },
                               "size": 31
                             }
                           }
                         }
                       }'
                       '''

nf_recent_days_query1 = "curl -u admin:adminamkt -POST '%s/%s/_search' -d '{ " % (URL, NF_INDEX1) + \
                        '''
                          "size": 0,
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "range": {
                                    "publish_date_date": {
                                      "from": "%s",
                                      "to": "%s"
                                    }
                                  }
                                }
                              ]
                            }
                          },
                          "aggregations": {
                            "post1": {
                              "terms": {
                                "field": "publish_date_date",
                                "order": {
                                  "_term": "desc"
                                },
                                "size": 31
                              }
                            }
                          }
                        }'
                        '''

nf_recent_hours_query = "curl -u admin:adminamkt -POST '%s/%s/_search' -d '{ " % (URL, NF_INDEX) + \
                        '''
                          "size": 0,
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "range": {
                                    "publish_date_date": {
                                      "from": "%s",
                                      "to": "%s"
                                    }
                                  }
                                }
                              ]
                            }
                          },
                          "aggregations": {
                            "post1": {
                              "terms": {
                                "field": "publish_date_hour",
                                "order": {
                                  "_term": "desc"
                                },
                                "size": 48
                              }
                            }
                          }
                        }'
                        '''

nf_recent_hours_query1 = "curl -u admin:adminamkt -POST '%s/%s/_search' -d '{ " % (URL, NF_INDEX1) + \
                         '''
                           "size": 0,
                           "query": {
                             "bool": {
                               "must": [
                                 {
                                   "range": {
                                     "publish_date_date": {
                                       "from": "%s",
                                       "to": "%s"
                                     }
                                   }
                                 }
                               ]
                             }
                           },
                           "aggregations": {
                             "post1": {
                               "terms": {
                                 "field": "publish_date_hour",
                                 "order": {
                                   "_term": "desc"
                                 },
                                 "size": 48
                               }
                             }
                           }
                         }'
                         '''

wx_recent_days_query = "curl -u admin:adminamkt -POST '%s/%s/wechat/_search' -d '{ " % (URL, WX_INDEX) + \
                       '''
                         "size": 0,
                         "query": {
                           "bool": {
                             "must": [
                               {
                                 "range": {
                                   "publish_date_date": {
                                     "from": "%s",
                                     "to": "%s"
                                   }
                                 }
                               }
                             ]
                           }
                         },
                         "aggregations": {
                           "post1": {
                             "terms": {
                               "field": "publish_date_date",
                               "order": {
                                 "_term": "asc"
                               },
                               "size": 31
                             }
                           }
                         }
                       }'
                       '''


def query_last_hours(recent_hours_query, hours=48):
    delta = datetime.timedelta(hours=hours)
    enddate = datetime.datetime.now()
    startdate = enddate - delta

    cmd = recent_hours_query % (startdate.strftime(DAY_FORMAT), enddate.strftime(DAY_FORMAT))
    print(cmd)
    fp = os.popen(cmd)
    j = fp.read()
    fp.close()
    res = json.loads(j)
    total = res["hits"]["total"]
    result = []
    for bucket in res["aggregations"]["post1"]["buckets"]:
        result.append((bucket["key"], bucket["doc_count"]))
    ret = (total, result)
    return ret


def query_last_days(recent_days_query, days=5):
    delta = datetime.timedelta(days=days)
    enddate = datetime.datetime.now()
    startdate = enddate - delta

    cmd = recent_days_query % (startdate.strftime(DAY_FORMAT), enddate.strftime(DAY_FORMAT))
    print(cmd)
    fp = os.popen(cmd)
    j = fp.read()
    fp.close()
    res = json.loads(j)
    total = res["hits"]["total"]
    result = []
    for bucket in res["aggregations"]["post1"]["buckets"]:
        result.append((bucket["key"], bucket["doc_count"]))
    ret = (total, result)
    return ret


def query_month_total(month_query, d=datetime.datetime.now()):
    prefix = d.strftime(MONTH_FORMAT)
    cmd = month_query % (prefix, prefix)
    print(cmd)
    fp = os.popen(cmd)
    j = fp.read()
    fp.close()
    res = json.loads(j)
    total = res["hits"]["total"]
    return total


def query_last_months(month_query, m=3):
    d = datetime.datetime.now()
    mon = d.month

    ret = []
    for i in range(m):
        year = d.year
        if mon == 0:
            year -= 1
            mon = 12
        d = datetime.date(year, mon, 1)
        total = query_month_total(month_query, d)
        ret.append((d.strftime(MONTH_FORMAT), total))
        mon -= 1
    return ret


def track_all_text():
    res = track_all()
    ret = []
    for key, qres in res:
        ret.append(">>>> " + key)

        total, dist = qres
        for t, v in dist:
            ret.append("    " + t + " => " + str(v))

        ret.append("===================")
    return "\n".join(ret)


def track_all():
    res = []

    # weibo
    tbl = WB_INDEX
    res.append((tbl + MSG_DAY, query_last_days(wb_recent_days_query)))
    res.append((tbl + MSG_MONTH, query_last_months(wb_month_query)))

    # newsforum
    tbl = NF_INDEX
    res.append((tbl + MSG_DAY, query_last_days(nf_recent_days_query)))
    res.append((tbl + MSG_MONTH, query_last_months(nf_month_query)))
    res.append((tbl + MSG_HOUR, query_last_hours(nf_recent_hours_query)))

    # wechat
    tbl = WX_INDEX
    res.append((tbl + MSG_DAY, query_last_days(wx_recent_days_query)))

    print("==========================")
    for tpl in res: print(tpl)
    return res


if __name__ == "__main__":
    # ret = query_last_days(nf_recent_days_query, days=10)
    ret = query_last_hours(nf_recent_hours_query)
    # ret = query_last_hours(nf_recent_hours_query1)
    print(ret)
