--- 视频
drop table if exists DS_BANYAN_VIDEO_POST;
CREATE table DS_BANYAN_VIDEO_POST (
   "pk" VARCHAR PRIMARY KEY,     -- unique_id
   "r"."cat_id" VARCHAR,         -- 分类ID,  1-电商 ,2-论坛,3-新闻,4-视频,5-渠道,6-博客,7-问答,8-其他
   "r"."is_parent" VARCHAR,      -- 1
    "r"."item_id" VARCHAR ,
    "r"."title" VARCHAR,          -- 标题
    "r"."content" VARCHAR,        -- 正文
    "r"."author" VARCHAR,         -- 作者
    "r"."album" VARCHAR ,       -- 视频所属专辑(optional)
    "r"."view_cnt" VARCHAR,     -- (Integer)阅读数/查看数, 只有主贴才有
    "r"."review_cnt" VARCHAR,   -- (Integer)评论数/回复数 , 只有主贴才有
    "r"."like_cnt" VARCHAR ,
    "r"."dislike_cnt" VARCHAR ,
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,
    "r"."channel" VARCHAR ,       -- type in raw json， 渠道
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."sentiment" VARCHAR,   -- 情感
    "r"."keywords" VARCHAR,    -- 关键词
    "r"."fingerprint" VARCHAR, -- 关键词指纹
    "r"."is_ad" VARCHAR ,      -- 是否广告
    "r"."is_robot" VARCHAR,     -- 是否机器人
    "r"."crawl_kw" VARCHAR ,      -- 爬虫关键词
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)
    "r"."sourceCrawlerId" VARCHAR,-- 内部字段,  爬虫ID (模板ID)
    "r"."taskId" VARCHAR  --内部字段, 任务配置ID/频道ID/版块ID
) COMPRESSION='SNAPPY' split on ('00','10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0')
;


drop table if exists DS_BANYAN_VIDEO_COMMENT;
CREATE table DS_BANYAN_VIDEO_COMMENT (
   "pk" VARCHAR PRIMARY KEY,     -- unique_id
   "r"."cat_id" VARCHAR,         -- 分类ID,  1-电商 ,2-论坛,3-新闻,4-视频,5-渠道,6-博客,7-问答,8-其他
   "r"."parent_id" VARCHAR, -- parent unique_id
   "r"."is_parent" VARCHAR,      -- 0
   "r"."item_id" VARCHAR,
    "r"."title" VARCHAR,          -- 标题
    "r"."content" VARCHAR,        -- 正文
    "r"."author" VARCHAR,         -- 作者
    "r"."province" VARCHAR ,    -- 评论者省份
    "r"."city" VARCHAR,         -- 评论者城市
    "r"."view_cnt" VARCHAR,     -- (Integer)阅读数/查看数, 只有主贴才有
    "r"."review_cnt" VARCHAR,   -- (Integer)评论数/回复数 , 只有主贴才有
    "r"."like_cnt" VARCHAR ,
    "r"."dislike_cnt" VARCHAR ,
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,  --论坛名字
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."sentiment" VARCHAR,   -- 情感
    "r"."keywords" VARCHAR,    -- 关键词
    "r"."fingerprint" VARCHAR, -- 关键词指纹
    "r"."is_ad" VARCHAR ,      -- 是否广告
    "r"."is_robot" VARCHAR,     -- 是否机器人
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)
    "r"."sourceCrawlerId" VARCHAR,-- 内部字段,  爬虫ID (模板ID)
    "r"."taskId" VARCHAR  --内部字段, 任务配置ID/频道ID/版块ID
) COMPRESSION='SNAPPY' split on ('00','10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0')
;