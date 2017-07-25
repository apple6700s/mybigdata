#!/usr/bin/env bash
nohup sh run.sh com.datastory.banyan.weibo.flush_es.CommentFlusher $@ > ../flush_cmt.log &