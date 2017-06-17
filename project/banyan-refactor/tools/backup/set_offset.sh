#!/usr/bin/env bash

UTIME='20161227000000'

# banyan
python ~/script/set_offset.py consumer.group.weibo.all.refactor topic_rhino_weibo_all $UTIME
python ~/script/set_offset.py consumer.group.weibo.update.refactor.v1 topic_rhino_weibo_count $UTIME
python ~/script/set_offset.py consumer.group.weibo.comment.refactor.v1 topic_rhino_weibo_comment  $UTIME
python ~/script/set_offset.py consumer.group.news.all.refactor topic_rhino_news_bbs_all_v3 $UTIME
python ~/script/set_offset.py consumer_group_wx_cnt topic_rhino_wx_cnt $UTIME

# rhino
python ~/script/set_offset.py consumer.group.weibo.update topic_rhino_weibo_count $UTIME
python ~/script/set_offset.py consumer.group.weibo.comment.v20161104 topic_rhino_weibo_comment $UTIME
python ~/script/set_offset.py consumer.group.news.all.reload.20161106.1 topic_rhino_news_bbs_all_v3 $UTIME
python ~/script/set_offset.py consumer.group.weibo.all topic_rhino_weibo_all $UTIME

# remend
python ~/script/set_offset.py consumer.group.weibo.all.remend.1214 topic_rhino_weibo_all_remend $UTIME
