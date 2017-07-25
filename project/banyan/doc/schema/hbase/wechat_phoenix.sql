drop table if EXISTS DS_BANYAN_WECHAT_CONTENT;
create TABLE DS_BANYAN_WECHAT_CONTENT (
  -- mp 公共字段
  "pk" VARCHAR primary key,       -- item_id
  "r"."publish_date" varchar,   -- 发表日期
  "r"."update_date" varchar,
  "r"."title" varchar,      -- 标题
  "r"."content" varchar,    -- 内容
  "r"."author" VARCHAR,     -- 微信公众号
  "r"."url" VARCHAR ,       -- 文章链接
  "r"."like_cnt" VARCHAR ,  -- 点赞数
  "r"."view_cnt" VARCHAR ,  -- 阅读数
  "r"."is_original" VARCHAR, -- 是否原创
  "r"."thumbnail" VARCHAR,  -- 文章缩略图
  "r"."brief" VARCHAR,    -- 文章简介
  "r"."other_data" VARCHAR ,  -- 额外的一些消息，会存一个map json

  -- analyz
  "r"."keywords" VARCHAR ,  -- top 5 keywords
  "r"."fingerprint" VARCHAR ,  -- keywords fingerprint md5
  "r"."sentiment" VARCHAR , -- 情感
  "r"."is_ad" VARCHAR, --  是否为广告

  -- 微信特殊字段
  "r"."src_url" varchar,    -- 源链接（应用组专用）
  "r"."article_id" VARCHAR ,  -- 文章id
  "r"."biz" varchar,      -- 微信公众号biz id
  "r"."idx" varchar,      -- 微信 idx
  "r"."sn" varchar,       -- 微信 sn
  "r"."mid" varchar,      -- 微信 mid
  "r"."wxid" varchar,     -- 微信公众号 id
  "r"."wx_author" VARCHAR,   -- 文章作者

   -- 元信息字段，表示数据来源，用爬虫taskId表示，存30个version
  "m"."taskId" VARCHAR
) split on ('01','02','03','04','05','06','07','08','09','0a','0b','0c','0d','0e','0f','10','11','12','13','14','15','16','17','18','19','1a','1b','1c','1d','1e','1f','20','21','22','23','24','25','26','27','28','29','2a','2b','2c','2d','2e','2f','30','31','32','33','34','35','36','37','38','39','3a','3b','3c','3d','3e','3f','40','41','42','43','44','45','46','47','48','49','4a','4b','4c','4d','4e','4f','50','51','52','53','54','55','56','57','58','59','5a','5b','5c','5d','5e','5f','60','61','62','63','64','65','66','67','68','69','6a','6b','6c','6d','6e','6f','70','71','72','73','74','75','76','77','78','79','7a','7b','7c','7d','7e','7f','80','81','82','83','84','85','86','87','88','89','8a','8b','8c','8d','8e','8f','90','91','92','93','94','95','96','97','98','99','9a','9b','9c','9d','9e','9f','a0','a1','a2','a3','a4','a5','a6','a7','a8','a9','aa','ab','ac','ad','ae','af','b0','b1','b2','b3','b4','b5','b6','b7','b8','b9','ba','bb','bc','bd','be','bf','c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','ca','cb','cc','cd','ce','cf','d0','d1','d2','d3','d4','d5','d6','d7','d8','d9','da','db','dc','dd','de','df','e0','e1','e2','e3','e4','e5','e6','e7','e8','e9','ea','eb','ec','ed','ee','ef','f0','f1','f2','f3','f4','f5','f6','f7','f8','f9','fa','fb','fc','fd','fe','ff');


drop table if EXISTS DS_BANYAN_WECHAT_MP_V1;
create table  DS_BANYAN_WECHAT_MP_V1 (
  "pk" VARCHAR primary key, -- md5前3位 + biz
  "r"."name" varchar,     -- 公众号名字
  "r"."desc" VARCHAR,     -- 公众号描述
  "r"."update_date" varchar,
  "r"."fans_cnt" varchar,   -- 粉丝数 （注，暂不可用）
  "r"."verify_status" varchar, -- -1 未认证 ，0 审核中， 1 已认证 ，2 微信认证，3 微博认证，4 手机认证， 5 邮箱认证, 6 实名认证，7 教育认证，8 公司认证

  -- 微信特殊字段
  "r"."biz" varchar,    -- 微信公众号biz id
  "r"."wxid" varchar,   -- 微信公众号 id
  "r"."open_id" VARCHAR, -- 搜狗微信 openid

   -- 元信息字段，表示数据来源，用爬虫taskId表示，存30个version
  "m"."taskId" VARCHAR
) COMPRESSION='SNAPPY' split on ('00','10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0');