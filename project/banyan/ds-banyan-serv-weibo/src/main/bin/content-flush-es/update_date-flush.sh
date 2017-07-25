#!/usr/bin/env bash
cd ..

#sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161025000000 20161100000000
#sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161020000000 20161025000000
#sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161015000000 20161020000000
#sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161010000000 20161015000000
#sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161005000000 20161010000000

sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161001000000 20161001120000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161001120000 20161002000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161002000000 20161002120000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161002120000 20161003000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161003000000 20161003120000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161003120000 20161004000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161004000000 20161004120000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161004120000 20161005000000

sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160925000000 20161000000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160920000000 20160925000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160915000000 20160920000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160910000000 20160915000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160905000000 20160910000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160900000000 20160905000000

# sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161100000000 20161200000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160800000000 20160815000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20160815000000 20160900000000
sh run.sh com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR 20161200000000 20161205000000
