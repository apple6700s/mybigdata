-- 主贴
drop table DS_BANYAN_NEWSFORUM_POST_V1;
CREATE table DS_BANYAN_NEWSFORUM_POST_V1 (
    "pk" VARCHAR PRIMARY KEY,     -- item_id
--     "r"."item_id" VARCHAR,        -- 数据ID : 论坛- md5(url+作者+内容+时间 ) , 新闻- md5(url+作者+内容)
    "r"."cat_id" VARCHAR,         -- 分类ID,  1-电商 ,2-论坛,3-新闻,4-视频,5-渠道,6-博客,7-问答,8-其他
    "r"."title" VARCHAR,          -- 标题
    "r"."content" VARCHAR,        -- 正文
    "r"."all_content" VARCHAR,        -- 正文 + 第一页回帖
    "r"."is_main_post" VARCHAR ,  -- 是否主贴， 0 为非主贴，1或空为主贴
    "r"."author" VARCHAR,         -- 作者
    "r"."view_cnt" VARCHAR,     -- (Integer)阅读数/查看数, 只有主贴才有
    "r"."review_cnt" VARCHAR,   -- (Integer)评论数/回复数 , 只有主贴才有
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,  --论坛名字
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)
    "r"."source" VARCHAR , --新闻来源
    "r"."introduction" VARCHAR , -- 导读
    "r"."origin_label" VARCHAR , -- 新闻标签
    "r"."same_html_count" VARCHAR , -- 百度新闻上的相似新闻数
    "r"."is_digest" VARCHAR , --是否精华帖
    "r"."is_hot" VARCHAR , --是否火贴
    "r"."is_top" VARCHAR , --是否置顶
    "r"."is_recom" VARCHAR , -- 是否推荐
    "r"."sourceCrawlerId" VARCHAR,-- 内部字段,  爬虫ID (模板ID)
    "r"."taskId" VARCHAR , --内部字段, 任务配置ID/频道ID/版块ID
    -- 分析字段
    "r"."sentiment" VARCHAR , -- 情感值
    "r"."keywords" VARCHAR , -- 关键词，最多前5个
    "r"."fingerprint" VARCHAR , -- md5(keywords)
    "r"."is_ad" VARCHAR, --  是否为广告
    "r"."is_robot" VARCHAR --  是否为水军、刷量等
) split on ('01','02','03','04','05','06','07','08','09','0a','0b','0c','0d','0e','0f','10','11','12','13','14','15','16','17','18','19','1a','1b','1c','1d','1e','1f','20','21','22','23','24','25','26','27','28','29','2a','2b','2c','2d','2e','2f','30','31','32','33','34','35','36','37','38','39','3a','3b','3c','3d','3e','3f','40','41','42','43','44','45','46','47','48','49','4a','4b','4c','4d','4e','4f','50','51','52','53','54','55','56','57','58','59','5a','5b','5c','5d','5e','5f','60','61','62','63','64','65','66','67','68','69','6a','6b','6c','6d','6e','6f','70','71','72','73','74','75','76','77','78','79','7a','7b','7c','7d','7e','7f','80','81','82','83','84','85','86','87','88','89','8a','8b','8c','8d','8e','8f','90','91','92','93','94','95','96','97','98','99','9a','9b','9c','9d','9e','9f','a0','a1','a2','a3','a4','a5','a6','a7','a8','a9','aa','ab','ac','ad','ae','af','b0','b1','b2','b3','b4','b5','b6','b7','b8','b9','ba','bb','bc','bd','be','bf','c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','ca','cb','cc','cd','ce','cf','d0','d1','d2','d3','d4','d5','d6','d7','d8','d9','da','db','dc','dd','de','df','e0','e1','e2','e3','e4','e5','e6','e7','e8','e9','ea','eb','ec','ed','ee','ef','f0','f1','f2','f3','f4','f5','f6','f7','f8','f9','fa','fb','fc','fd','fe','ff') COMPRESSION='SNAPPY';

CREATE index IDX_NFPOST_UDATE ON DS_BANYAN_NEWSFORUM_POST_V1 ("r"."update_date") VERSION=1, COMPRESSION='SNAPPY', SALT_BUCKETS=2;

-----------------------------------------
-- 回帖，评论
drop table DS_BANYAN_NEWSFORUM_COMMENT_V1;
CREATE table DS_BANYAN_NEWSFORUM_COMMENT_V1 (
    "pk" VARCHAR PRIMARY KEY,     -- item_id
--     "r"."item_id" VARCHAR,        -- 数据ID : 论坛- md5(url+作者+内容+时间 ) , 新闻- md5(url+作者+内容)
    "r"."parent_post_id" VARCHAR, -- parent item_id
    "r"."cat_id" VARCHAR,         -- 分类ID,  1-电商 ,2-论坛,3-新闻,4-视频,5-渠道,6-博客,7-问答,8-其他
    "r"."title" VARCHAR,          -- 标题
    "r"."is_main_post" VARCHAR ,  -- 是否主贴， 0 为非主贴，1或空为主贴
    "r"."content" VARCHAR,        -- 正文
    "r"."author" VARCHAR,         -- 作者
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,  --论坛名字
    "r"."source" VARCHAR , --新闻来源
    "r"."sourceCrawlerId" VARCHAR,-- 内部字段,  爬虫ID (模板ID)
    "r"."taskId" VARCHAR , --内部字段, 任务配置ID/频道ID/版块ID
    -- 分析字段
    "r"."sentiment" VARCHAR , -- 情感值
    "r"."keywords" VARCHAR , -- 关键词，最多前5个
    "r"."fingerprint" VARCHAR , -- md5(keywords)
    "r"."is_ad" VARCHAR, --  是否为广告
    "r"."is_robot" VARCHAR --  是否为水军、刷量等
) split on ('01','02','03','04','05','06','07','08','09','0a','0b','0c','0d','0e','0f','10','11','12','13','14','15','16','17','18','19','1a','1b','1c','1d','1e','1f','20','21','22','23','24','25','26','27','28','29','2a','2b','2c','2d','2e','2f','30','31','32','33','34','35','36','37','38','39','3a','3b','3c','3d','3e','3f','40','41','42','43','44','45','46','47','48','49','4a','4b','4c','4d','4e','4f','50','51','52','53','54','55','56','57','58','59','5a','5b','5c','5d','5e','5f','60','61','62','63','64','65','66','67','68','69','6a','6b','6c','6d','6e','6f','70','71','72','73','74','75','76','77','78','79','7a','7b','7c','7d','7e','7f','80','81','82','83','84','85','86','87','88','89','8a','8b','8c','8d','8e','8f','90','91','92','93','94','95','96','97','98','99','9a','9b','9c','9d','9e','9f','a0','a1','a2','a3','a4','a5','a6','a7','a8','a9','aa','ab','ac','ad','ae','af','b0','b1','b2','b3','b4','b5','b6','b7','b8','b9','ba','bb','bc','bd','be','bf','c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','ca','cb','cc','cd','ce','cf','d0','d1','d2','d3','d4','d5','d6','d7','d8','d9','da','db','dc','dd','de','df','e0','e1','e2','e3','e4','e5','e6','e7','e8','e9','ea','eb','ec','ed','ee','ef','f0','f1','f2','f3','f4','f5','f6','f7','f8','f9','fa','fb','fc','fd','fe','ff') COMPRESSION='SNAPPY';

CREATE index IDX_NFCMT_UDATE ON DS_BANYAN_NEWSFORUM_COMMENT_V1 ("r"."update_date") VERSION=1, COMPRESSION='SNAPPY', SALT_BUCKETS=2;
CREATE index IDX_NFCMT_PARENT ON DS_BANYAN_NEWSFORUM_COMMENT_V1 ("r"."parent_post_id") VERSION=1, COMPRESSION='SNAPPY', SALT_BUCKETS=2;