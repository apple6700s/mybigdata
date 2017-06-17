共有字段：
{
    "cat_id":"1"    //1代表电商，4代表视频
    "site_id":"1"   //站点id
}
视频详情：
{     
    "unique_id": "",    //视频详情全局唯一id
    "item_id": "82138d620df35c5488092dadc62a5673",
    "site": "爱奇艺",
    "title": "射雕英雄傳第1集",
    "content": "郭夫人和杨夫人同时怀有身孕",
    "url": "http://www.iqiyi.com/v_19rr9x93h8.html",
    "author": "連續劇",
    "view_count": "4183966718",
    "like_count": "88404",
    "dislike_count": "36250",
    "review_count": "2050",
    "type": "hunantv2014",
    "publish_date": "20170109234515", //优酷,从列表页爬虫获得，只有前20个视频可以抓到发布时间    芒果TV,PPTV聚力,抓取不到,页面上没看到日期，可能改版
    "update_date": "20170410154227",
    "album_title":"专辑名称，xx",  //不是所有视频都有
}
视频评论：
{
    "unique_id": "",            //视频评论全局唯一id
    "unique_parent_id": "",     //关联视频详情
    "site": "爱奇艺",
    "title": "那片星空那片海第1集",
    "url": "http://www.iqiyi.com/v_19rrat6mqs.html",
    "content": "好好看",
    "author": "春雨_wx4881",
    "view_count": "0",
    "like_count": "0",
    "dislike_count": "0",
    "review_count": "0",
    "province": "广东",
    "city": "中山",
    "publish_date": "20170219215924",
    "update_date": "20170410180048"
}
电商详情：
{
    "unique_id": "",    //电商详情全局唯一id
    "site": "麦乐购",
    "item_id": "26400",
    "item_title": "DEEPCOOL TESSERACT SW Mid Tower Computer Case Blue LED Fans Side Window Standard ATX chassis Technical Details Additional Information Warranty & Support Feedback",
    "item_url": "http://www.gou.com/product_26400.html",
    "item_image_url": "https://file1.m6go.com/5zmL_LZ56~uCDsEGR4RtBF/50", 
    "sell_count_recent": "22", //最近销量？  用到的爬虫：淘宝商品单页、淘宝商品TOP N搜索,淘宝商品详情,淘宝商品月销量 天猫商品列表、天猫国际商品列表,天猫销量信息
    "sell_count_total": "28", //总销量
    "sell_count_month": "14"    //用到的爬虫：聚美优品商品信息
    "repertory": "广东广州",
    "price": "￥ 1599",  // 字符串！
    "promo_price": "599",   // 字符串！
    "promotion_info": "[{\"加价购\":\"【酒饮换购】购买酒水指定商品即可到购物车参加加价换购，数量有限换完为止！\"}]",   //json list<map>的形式
    "review_count": "12",
    "good_count": "392",
    "poor_count": "1", //差评数
    "shop_name": "中粮我买网",
    "other_data": "{\"适合肤质\":\"一般肤质，尤其想要补水的肌肤\",\"产品规格\":\"30g*5片\"}",   //json map的形式
    "update_date": "20170410193222",
    "content": "Valle Black Bonded Leather Dining Chairs (Set of 2)",
    "shop_id": "1935567546",
    "platform_score": "98.9",
}
电商评论：
{
    "unique_id": "",            //电商评论全局唯一id
    "unique_parent_id": "",     //关联电商详情
    "comment_id": "ce73b64186a83392c18f71e986296528",
    "item_id": "af230467b00adac8eed6124e2c09b87f",  //所属的商品的id
    "site": "小红书",
    "item_title": "亨氏 番茄酱 300g/瓶",
    "shop_name": "君乐宝官方旗舰店",
    "content": "回复 这DD: 个人感觉还好😂",
    "author": "一生歡喜",
    "reference_name": "德国博朗婴幼儿耳温枪IRT6020",
    "reference_date": "20160904175820",
    "score": "5",
    "publish_date": "20170107130300",
    "update_date": "20170410200551",
    "image_url_list": "[]", //评论里的图片， json list格式 
    "shop_category": "天猫旗舰店",
    "url": "http://m.xiaohongshu.com/discovery/item/586fbd3dd2c8a51d8e9778ee" // 商品详情页链接
}





// real example
// ecom comment
{
    "type": 1,
    "depth": 4,
    "srcInfo": {
        "FILTER_TYPE": "1",
        "keyword": "味事达 生抽",
        "TOP_N": "20",
        "crawler": "12",
        "start_date": "20170605120000",
        "end_date": "20170605170000",
        "lang": "",
        "time_zone": "PRC"
    },
    "jobName": "temp_ecomm_20170605183945_706_56",
    "srcId": 15,
    "info": {
        "taskId": "102155",
        "site": "麦乐购",
        "comment_id": "eedd461bec29d5a6c923bf7d242f8254",
        "publish_date": "20170605131400",
        "reference_name": "荷兰牛栏奶粉Nutrilon 5段（2-7岁）800g",
        "item_title": "荷兰牛栏奶粉Nutrilon 5段（2-7岁）800g（海外版）",
        "image_url_list": "[]",
        "score": "5",
        "site_id": "-1",
        "lang": "",
        "url": "http://www.gou.com/product_591.html",
        "cat_id": "1",
        "unique_id": "8defee268cbbbb9f061bacd481858005",
        "unique_parent_id": "c5c14403a55e8bf2eb7b42f3516ea1ca",
        "content": "很好，一直吃的这个",
        "time_zone": "PRC",
        "title": "荷兰牛栏奶粉Nutrilon 5段（2-7岁）800g（海外版）",
        "_html_": "避免过长，省略...",
        "comment_date": "20170605131400",
        "item_id": "591",
        "update_date": "20170605185334",
        "parent_id": "591"
    },
    "cacheObject": {}
}

// ecom item
{
    "type": 1,
    "depth": 3,
    "srcInfo": {
        "FILTER_TYPE": "1",
        "keyword": "卡夫 色拉酱",
        "TOP_N": "20",
        "crawler": "12",
        "start_date": "20170605120000",
        "end_date": "20170605170000",
        "lang": "",
        "time_zone": "PRC"
    },
    "jobName": "temp_ecomm_20170605183945_706_56",
    "cids": [
        15
    ],
    "srcId": 14,
    "info": {
        "taskId": "102155",
        "site": "麦乐购",
        "promo_price": "26",
        "review_count": "5",
        "item_image_url": "https://file.m6go.com/0uXLBBx0RkIBUVmHD~3PvA/50",
        "site_id": "-1",
        "promotion_info": "[{\"满额赠\":\"登录 满199.00元即赠热销商品，购物车领取赠品\"}]",
        "lang": "",
        "item_url": "http://www.gou.com/product_13961.html",
        "cat_id": "1",
        "unique_id": "c95d1c3e238e5c2ee0b25a43a3f78fdb",
        "sell_count": "186",
        "time_zone": "PRC",
        "title": "贝克曼博士Dr．Beckmann衣物除垢去污 酱渍与咖喱污渍克星50mL",
        "_html_": "避免过长，省略...",
        "price": "￥ 34",
        "item_id": "13961",
        "activity_info": "",
        "update_date": "20170605184052",
        "sell_count_total": "186"
    },
    "cacheObject": {}
}