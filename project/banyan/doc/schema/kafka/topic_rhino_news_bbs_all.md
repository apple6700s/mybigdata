```json
{
  "cat_id":"2",
  "sourceCrawlerId":"1102",
  "msgDepth":"2",
  "page_id":"6a6ee06f04b1325f89a6f2a24d3038bb",
  "update_date":"20161011103305",
  "site_id":"100579",
  "msgType":"1",
  "full_url":"http://bbs.chetxia.com/board_615",
  "jobName":"increment_job_20161011102622_452_99",
  "taskId":"....",
  // json string
  "post":"",
  // json string
  "replys":""
}

// post json example
{
"taskId":"9279",
"sourceCrawlerId":"1102",
"publish_date":"20160826200846",
"review_count":"22",
"site_id":"100579",
"_text_":"转：草原情，奔 腾心",
"url":"http://bbs.chetxia.com/615/1130_22804871_22804871.htm",
"cat_id":"2",
"content":"因为我们今生有缘，我和你有个约定 等到草原最美的时候陪你一起看草原　(2小时翻越大青山来到希拉穆仁草原) 去看那篮篮天，去看那远飞的雁 陪你一起看草原让爱留心间 你是父亲心爱的俊马，你是母亲放飞的雄鹰 感谢内蒙古呼市奔腾4S店，华美无界改灯，华美古德安全管家，大力支持与赞助! 2016年7月31日奔腾X80车友会携手奔腾4S店，无界，古德！共同渡过了美好的草原之旅！欢迎更多X80车友加入我们一起奔腾吧…",
"author":"yangxirun",
"title":"转：草原情，奔腾心",
"item_id":"fdd0811b3e1a36b1edb13322dfd82164",
"page_id":"6a6ee06f04b1325f89a6f2a24d3038bb",
"update_date":"20161011103305",
"is_main_post":"1",
"view_count":"2122",
"origin_label":"",
"introduction": "这篇新闻什么都没说",
"same_html_count": "0"
}

// replys json example
[
  {
    "taskId":"9279",
    "sourceCrawlerId":"1102",
    "publish_date":"20160826221607",
    "site_id":"100579",
    "_text_":"转：草原情，奔 腾心",
    "url":"http://bbs.chetxia.com/615/1130_22804871_22804871.htm",
    "cat_id":"2",
    "content":"吃好喝好玩好，要不要这么赞？",
    "author":"薇薇安安",
    "title":"转：草原情，奔 腾心",
    "item_id":"3abd36ddb0470d89ebd769c27114297a",
    "page_id":"6a6ee06f04b1325f89a6f2a24d3038bb",
    "update_date":"20161011103305",
    "is_main_post":"0",
    "parent_id":"fdd0811b3e1a36b1edb13322dfd82164"
  }
]
```

```java
// 旧版 etl object
LongTextAllFields {
    private String pk;     //md5(item_id)前2位 + site_id + item_id
    private String itemId;     //数据ID : 论坛- md5(url+作者+内容+时间 ) , 新闻- md5(url+作者+内容)
    private String isMainPost;     //是否为主贴
    private String title;     //标题
    private String content;     //正文
    private String author;     //作者
    private String viewCount;     //(Integer)阅读数/查看数, 只有主贴才有
    private String reviewCount;     //(Integer)评论数/回复数 , 只有主贴才有
    private String url;     //url 页面URL
    private String source;  // 新闻论坛来源
    private String siteId;     //站点ID
    private String catId;     //分类ID,  1-电商 ,2-论坛,3-新闻,4-视频,5-渠道,6-博客,7-问答,8-其他
    private String publishDate;     //发布时间
    private String updateDate;     //数据抓取时间
    private String pageId;     //所属网页ID,  md5( 去除#字串的url)
    private String parentId;     //主贴的item_id
    private String sourceCrawlerId;     //内部字段,  爬虫ID (模板ID)
    private String taskId;     //内部字段, 任务配置ID/频道ID/版块ID
    private String sentiment;     //情感值
    private String keywords;     //关键词，最多前5个
    private String fingerprint;     //md5(keywords)
    private String isAd;     //是否为广告
    private String isDigest; //是否为精华帖
    private String isHot;   //是否火贴
    private String isTop;   //是否置顶
    private String isRecom;  //是否推荐
    private String isRobot; // 是否水军
}
```
