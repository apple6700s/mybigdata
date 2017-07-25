```json
{
  "uid":"5456403543",
  "sourceCrawlerId":"1417",
  "msgDepth":"1",
  "site_id":"101993",
  "jobName":"MIG_weibo_count_20161008121145_662_36",
  "attitudes":"0",
  "crawler":"1417",
  "cat_id":"9",
  "id":"4024651233274432",
  "end_date":"20201231000000",
  "reposts":"0",
  "update_date":"20161008122135",
  "msgType":"1",
  "mid":"4024651233274432",
  "mids":"4024651233274432",
  "start_date":"19700101000000",
  "comments":"0"
}
```

```java
// final etl object
WeiboUpFields {
    private String id;           //微博 id
    private String comments;     //微博评论数量
    private String reposts;      //微博转发数量
    private String attitudes;    //微博点赞数量
    private String userId;         //微博所属用户的id
    private String updateDate;  // 抓取时间
}
```