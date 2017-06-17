```json
// 完整的schema
{
  "sourceCrawlerId":"1759",
  "publish_date":"20161011161322",
  "msgDepth":"1",
  "site_id":"101993",
  "jobName":"weibo_new_batch_20161013121626_592_87",
  "cat_id":"9",
  "CONTROL_AMOUNT":"34",
  "uids":"1011697555",
  "end_date":"20161013120000",
  "update_date":"20161013121633",
  "msgType":"1",
  "mid":"4029426494867618",
  "start_date":"20161006120000",
  "json":"" // 字符串表示的json对象
}

// json 对象示例, 完整的WeiboStatus对象
// List<WeiboStatus>的真实数据示例可看 http://180.149.135.176/2/statuses/user_timeline.json?source=2323547071&gsid=4ufb2ba214rngqCXqingDeGim77&uid=1967472171&count=200&page=1

{
  "user":{
    "id":"1011697555",
    "screenName":"玲bu玲",
    "name":"玲bu玲",
    "province":400,
    "city":16,
    "location":"海外 其他",
    "description":"看起来不错",
    "url":"http://account.weibo.com/set/index",
    "profileImageUrl":"http://tva3.sinaimg.cn/crop.0.0.511.511.50/3c4d4793jw8f4df9znk8cj20e70e8gmt.jpg",
    "userDomain":"",
    "gender":"f",
    "followersCount":318,
    "friendsCount":161,
    "statusesCount":5222,
    "favouritesCount":154,
    "createdAt":"Feb 6, 2011 11:44:10 PM",
    "following":false,
    "verified":false,
    "verifiedType":220,
    "allowAllActMsg":false,
    "allowAllComment":true,
    "followMe":false,
    "avatarLarge":"http://tva3.sinaimg.cn/crop.0.0.511.511.180/3c4d4793jw8f4df9znk8cj20e70e8gmt.jpg",
    "onlineStatus":0,
    "biFollowersCount":90,
    "lang":"zh-cn",
    "verifiedReason":"",
    "weihao":"",
    "statusId":""
  },
  "createdAt":"Oct 11, 2016 4:13:22 PM",
  "id":"4029426494867618",
  "mid":"4029426494867618",
  "idstr":4029426494867618,
  "text":"宁静的世界被打破了，总是能发现你的蛛丝马迹[噢耶][噢耶]",
  "source":{
    "url":"http://weibo.com/",
    "relationShip":"nofollow",
    "name":"iPhone"
  },
  "favorited":false,
  "truncated":false,
  "inReplyToStatusId":-1,
  "inReplyToUserId":-1,
  "inReplyToScreenName":"",
  "thumbnailPic":"",
  "bmiddlePic":"",
  "originalPic":"",
  "retweetedStatus":{
    "user":{
      "id":"3217179555",
      "screenName":"回忆专用小马甲",
      "name":"回忆专用小马甲",
      "province":100,
      "city":1000,
      "location":"其他",
      "description":"《愿无岁月可回头》",
      "url":"",
      "profileImageUrl":"http://tva3.sinaimg.cn/crop.0.0.640.640.50/bfc243a3jw8efzr4c9ajij20hs0hsaav.jpg",
      "userDomain":"",
      "gender":"m",
      "followersCount":27259842,
      "friendsCount":619,
      "statusesCount":14720,
      "favouritesCount":4520,
      "createdAt":"Jan 10, 2013 1:37:59 PM",
      "following":false,
      "verified":true,
      "verifiedType":0,
      "allowAllActMsg":true,
      "allowAllComment":true,
      "followMe":false,
      "avatarLarge":"http://tva3.sinaimg.cn/crop.0.0.640.640.180/bfc243a3jw8efzr4c9ajij20hs0hsaav.jpg",
      "onlineStatus":0,
      "biFollowersCount":560,
      "lang":"zh-cn",
      "verifiedReason":"微博人气博主",
      "weihao":"",
      "statusId":""
    },
    "createdAt":"Sep 4, 2016 11:00:09 PM",
    "id":"4016120513886252",
    "mid":"4016120513886252",
    "idstr":4016120513886252,
    "text":"《愿无岁月可回头》即将印刷完成了，明晚八点预售，很快就能交到大家的手中。希望这本书，你会喜欢。希望你读到时，不会失望。#愿无岁月可回头#",
    "favorited":false,
    "truncated":false,
    "inReplyToStatusId":-1,
    "inReplyToUserId":-1,
    "inReplyToScreenName":"",
    "thumbnailPic":"http://ww2.sinaimg.cn/thumbnail/bfc243a3gw1f7hy4nk355j20go2d2wno.jpg",
    "bmiddlePic":"http://ww2.sinaimg.cn/bmiddle/bfc243a3gw1f7hy4nk355j20go2d2wno.jpg",
    "originalPic":"http://ww2.sinaimg.cn/large/bfc243a3gw1f7hy4nk355j20go2d2wno.jpg",
    "geo":"null",
    "latitude":-1,
    "longitude":-1,
    "repostsCount":18001,
    "commentsCount":12743,
    "attitudesCount":34308,
    // json string
    "annotations":"[{"client_mblogid":"iPhone-CC98EA2E-B904-4C0A-82CD-D81AD3084629"},{"mapi_request":true}]",
    "mlevel":0,
    "feature":0,
    "picUrls":[
      "http://ww2.sinaimg.cn/thumbnail/bfc243a3gw1f7hy4nk355j20go2d2wno.jpg"
    ],
    "visible":{
      "type":0,
      "list_id":0
    }
  },
  "geo":"null",
  "latitude":-1,
  "longitude":-1,
  "repostsCount":0,
  "commentsCount":0,
  "attitudesCount":0,
  "annotations":"",
  "mlevel":0,
  "feature":0,
  "pid":"4016131138849154",
  "visible":{
    "type":0,
    "list_id":0
  }
}
```

```java
// final etl object
// 其中Status和Status中的User均为新浪微博sdk提供的对象，与json一致。
ProcessedStatus {
    private Status originalStatus;
    private ProcessedStatus processedRetweetedStatus;

    //content 增加字段
    private String sentiment;           // 情感
    private String keywords;            // 关键词前5
    private String fingerprint;         // md5(keywords)
    private String topic;               //话题
    private String emoji;               //表情
    private String shortLink;           //短链
    private String originalLink;        //短链翻译还原成的长链接
    private String checkin;             //签到
    private String mention;             //提及，@的人，微博昵称
    private String forward;             //转发链，微博昵称
    private String isAd;                //是否为广告
    private String userType = "0";              //是否为水军
    private String selfContent;         //
    private String isReal;      // 微博是否为真实用户发的

    //user 增加字段
    private String constellation;       //星座，通过生日进行转化
}


```