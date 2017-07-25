```json
{
  "uid":"1773148625",
  "sourceCrawlerId":"250",
  "publish_date":"20160928231400",
  "msgDepth":"1",
  "site_id":"101993",
  "jobName":"MIG_weibo_comment_20161010030847_611_24",
  "taskId":"....",
  "crawler":"250",
  "cat_id":"9",
  "CONTROL_AMOUNT":"57",
  "end_date":"20161010235959",
  "update_date":"20161010103317",
  "msgType":"1",
  "mid":"4024821312336333",
  "start_date":"20160923000000",
  "parent_id":"4022055378315956",
  // json string
  "json":"{"createdAt":"Sep 28, 2016 11:14:00 PM","id":0,"mid":"4024821312336333","text":"蹦极","status":{"mid":"4022055378315956","idstr":0,"favorited":false,"truncated":false,"inReplyToStatusId":0,"inReplyToUserId":0,"latitude":-1.0,"longitude":-1.0,"repostsCount":0,"commentsCount":0,"attitudesCount":0,"mlevel":0,"feature":0},"user":{"id":"3563300751","screenName":"龅牙妹的小思绪","name":"龅牙妹的小思绪","province":0,"city":0,"followersCount":0,"friendsCount":0,"statusesCount":0,"favouritesCount":0,"following":false,"verified":false,"verifiedType":0,"allowAllActMsg":false,"allowAllComment":false,"followMe":false,"onlineStatus":0,"biFollowersCount":0}}"
}


// json example

{
  "createdAt":"Sep 28, 2016 11:14:00 PM",
  "id":0,
  "mid":"4024821312336333",
  "text":"蹦极",
  "status":{
    "mid":"4022055378315956",
    "idstr":0,
    "favorited":false,
    "truncated":false,
    "inReplyToStatusId":0,
    "inReplyToUserId":0,
    "latitude":-1,
    "longitude":-1,
    "repostsCount":0,
    "commentsCount":0,
    "attitudesCount":0,
    "mlevel":0,
    "feature":0
  },
  "user":{
    "id":"3563300751",
    "screenName":"龅牙妹的小思绪",
    "name":"龅牙妹的小思绪",
    "province":0,
    "city":0,
    "followersCount":0,
    "friendsCount":0,
    "statusesCount":0,
    "favouritesCount":0,
    "following":false,
    "verified":false,
    "verifiedType":0,
    "allowAllActMsg":false,
    "allowAllComment":false,
    "followMe":false,
    "onlineStatus":0,
    "biFollowersCount":0
  }
}
```

```java
// final etl object
WeiboCommentFields {
    private String pk;
    private String uid;
    private String name;
    private String commentId;
    private String mid;
    private String content;
    private String publishDate;
    private String updateDate;
    private String sentiment;
}
```