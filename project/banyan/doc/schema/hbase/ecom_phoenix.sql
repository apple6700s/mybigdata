--- 电商
drop table if exists DS_BANYAN_ECOM_ITEM;
CREATE table DS_BANYAN_ECOM_ITEM (
   "pk" VARCHAR PRIMARY KEY,     -- unique_id
   "r"."cat_id" VARCHAR,         -- 分类ID, // 1 电商 , 2 论坛 , 3 新闻 ,4 视频 , 5 点评 , 6 博客 , 7 问答 , 8 其他 , 9 微博 , 10 微信 , 11 百科
   "r"."is_parent" VARCHAR,      -- 1
    "r"."item_id" VARCHAR ,
    "r"."title" VARCHAR,          -- 标题 ,即商品名
    "r"."price" VARCHAR ,     -- 原本价格 (人民币)
    "r"."promo_price" VARCHAR ,   -- 促销折扣价（人民币）
    "r"."promo_info" VARCHAR ,   -- 促销文字信息
    "r"."sales_total_cnt" VARCHAR , -- 总销量
    "r"."sales_month_cnt" VARCHAR , -- 当月销量
    "r"."sales_rcnt30_cnt" VARCHAR , -- 近30day销量
    "r"."shop_name" VARCHAR ,    -- 店铺
    "r"."shop_id" VARCHAR ,    -- 店铺id
    "r"."shop_type" VARCHAR ,    -- 店铺类别（比如天猫分 旗舰店、自营、第三方等）
    "r"."platform_score" VARCHAR ,  -- 平台对商品评分 （具体含义找采集组）
--     "r"."view_cnt" VARCHAR,     -- (Integer)阅读数/查看数, 只有主贴才有
    "r"."review_cnt" VARCHAR,   -- (Integer)总评论数
    "r"."like_cnt" VARCHAR ,    -- 好评数
    "r"."dislike_cnt" VARCHAR , -- 差评数
    "r"."other_data" VARCHAR ,
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."pic_urls" VARCHAR,            -- 图片URL
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,
    "r"."repertory" VARCHAR,      -- 仓库
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."crawl_kw" VARCHAR ,      -- 爬虫关键词
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)

    -- 元信息字段，表示数据来源，用爬虫taskId表示，存30个version。实际数据库中可能还存在r列的taskId，为历史遗留，不再关注。
    "m"."taskId" VARCHAR  --内部字段, 任务配置ID/频道ID/版块ID
) COMPRESSION='SNAPPY' split on ('00','10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0')
;


drop table if exists DS_BANYAN_ECOM_COMMENT;
CREATE table DS_BANYAN_ECOM_COMMENT (
   "pk" VARCHAR PRIMARY KEY,     -- unique_id
   "r"."cat_id" VARCHAR,         -- 分类ID
   "r"."parent_id" VARCHAR,      -- parent unique_id
   "r"."is_parent" VARCHAR,      -- 0
    "r"."item_id" VARCHAR ,
    "r"."cmt_id" VARCHAR ,
    "r"."title" VARCHAR ,
    "r"."content" VARCHAR,        -- 评论正文
    "r"."author" VARCHAR,         -- 作者
    "r"."url" VARCHAR,            -- url 页面URL
    "r"."site_id" VARCHAR,        -- 站点ID
    "r"."site_name" VARCHAR,  --论坛名字
    "r"."publish_date" VARCHAR,   -- 发布时间
    "r"."update_date" VARCHAR,    -- 数据抓取时间
    "r"."page_id" VARCHAR, -- 所属网页ID,  md5( 去除#字串的url)

    "r"."shop_name" VARCHAR ,    -- 店铺
    "r"."shop_id" VARCHAR ,    -- 店铺id
    "r"."shop_type" VARCHAR ,    -- 店铺类别（比如天猫分 旗舰店、自营、第三方等）

    "r"."cmt_pic_urls" VARCHAR , -- 评论图片列表 json
    "r"."score" VARCHAR ,      -- 评论评分
    "r"."buy_item" VARCHAR ,  -- 购买商品具体型号
    "r"."buy_date" VARCHAR,    -- 购买时间

    "r"."sentiment" VARCHAR,   -- 情感
    "r"."keywords" VARCHAR,    -- 关键词
    "r"."fingerprint" VARCHAR, -- 关键词指纹
    "r"."is_ad" VARCHAR ,      -- 是否广告
    "r"."is_robot" VARCHAR,     -- 是否机器人

    -- 元信息字段，表示数据来源，用爬虫taskId表示，存30个version。实际数据库中可能还存在r列的taskId，为历史遗留，不再关注。
    "m"."taskId" VARCHAR  --内部字段, 任务配置ID/频道ID/版块ID
) COMPRESSION='SNAPPY' split on ('00','10','20','30','40','50','60','70','80','90','a0','b0','c0','d0','e0','f0')
;