package com.dt.mig.sync.base;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by abel.chan on 16/11/20.
 */
public class MigSyncConsts {


    //quartz
    public static final String ONE_DAY_TWO_TIME_QUARTZ = "one.day.two.time.quartz";//一天2次同步的表达式
    public static final String THREE_DAY_ONE_TIME_QUARTZ = "three.day.one.time.quartz"; //三天一次的同步表达式
    public static final String SEVEN_DAY_ONE_TIME_QUARTZ = "seven.day.one.time.quartz";//一周一次的同步表达式
    public static final String ONE_DAY_ONE_TIME_QUARTZ = "one.day.one.time.quartz";//一天一次的同步表达式
    public static final String DEFAULT_QUART_TIME = "0 0 2 */7 * ?";//默认七天执行一次


    // es const
    public static final String ES_CLUSTER_CONF = "es.cluster.name";
    public static final String ES_HOSTS = "es.hosts";
    public static final String ES_SCROLL_SIZE = "es.scroll.size";

    // ------------------ es reader ------------------
    //es index
    public static final String ES_WEIBO_INDEX = "dt-rhino-weibo-index";
    public static final String ES_WEIBO_CONMENT_INDEX = "dt-rhino-weibo-comment-index";
    public static final String ES_NEWS_FORUM_INDEX = "ds-banyan-newsforum-index";
    public static final String ES_NEWS_QUESTION_ANSWER_INDEX = "ds-banyan-newsforum-index";
    //es type
    public static final String ES_WEIBO_PARENT_TYPE = "user";
    public static final String ES_WEIBO_CHILD_TYPE = "weibo";
    public static final String ES_WEIBO_CONMENT_PARENT_TYPE = "weibo";
    public static final String ES_WEIBO_CONMENT_CHILD_TYPE = "comment";
    public static final String ES_NEWS_FORUM_PARENT_TYPE = "post";
    public static final String ES_NEWS_FORUM_CHILD_TYPE = "comment";
    public static final String ES_NEWS_QUESTION_ANSWER_PARENT_TYPE = "post";
    public static final String ES_NEWS_QUESTION_ANSWER_CHILD_TYPE = "comment";


    // hbase table 名称
    public static final String HBASE_WEIBO_CONTENT_TBL_NEW = "DS_BANYAN_WEIBO_CONTENT_V1";
    //public static final String HBASE_WEIBO_CONTENT_TBL = "dt.rhino.weibo.content.v2";
    //public static final String HBASE_WEIBO_USER_TBL = "dt.rhino.weibo.user.v2";
    public static final String HBASE_WEIBO_USER_TBL_NEW = "DS_BANYAN_WEIBO_USER";
    //    public static final String HBASE_NEWS_FORUM_TBL = "dt.rhino.sys.common.v7";
    public static final String HBASE_NEWS_FORUM_POST_TBL_NEW = "DS_BANYAN_NEWSFORUM_POST_V1";
    public static final String HBASE_NEWS_FORUM_COMMENT_TBL_NEW = "DS_BANYAN_NEWSFORUM_COMMENT_V1";
    //    public static final String HBASE_WEIBO_COMMENT_TBL = "dt.rhino.weibo.comment";
    public static final String HBASE_WEIBO_COMMENT_TBL_NEW = "DS_BANYAN_WEIBO_COMMENT";
    public static final String HBASE_TREND_TBL_NEW = "DS_BANYAN_COMMON_TREND";

    public static final String SYMBOL_AND = "\\&";

    public static final String SYMBOL_OR = "\\|";

    public static final String PARENT_ID = "_parent";

    public static final String DEFAULT_ANALY_FIELD = "content";//默认地分析字段


    //ES_COMMON
    public static final String ES_ROUTING_FIELD = "_routing";

    public static final String ES_ID_FIELD = "id";

    //------------------newsforum------------------

    public static final String ES_NEWS_FORUM_POST_PUBLISH_TIME = "publish_time";

    //post es
    public static final String ES_NEWS_FORUM_POST_PUBLISH_DATE = "publish_date";
    public static final String ES_NEWS_FORUM_POST_PUBLISH_DATE_DATE = "publish_date_date";
    public static final String ES_NEWS_FORUM_POST_CONTENT = "content";
    public static final String ES_NEWS_FORUM_POST_ALL_CONTENT = "all_content";
    public static final String ES_NEWS_FORUM_POST_CONTENT_LENGTH = "content_length";
    public static final String ES_NEWS_FORUM_POST_MAIN_POST = "main_post";
    public static final String ES_NEWS_FORUM_POST_TITLE = "title";
    public static final String ES_NEWS_FORUM_POST_TITLE_LENGTH = "title_length";
    public static final String ES_NEWS_FORUM_POST_TITLE_SOURCE = "title_source";
    public static final String ES_NEWS_FORUM_POST_MAIN_POST_LENGTH = "main_post_length";
    public static final String ES_NEWS_FORUM_POST_CAT_ID = "cat_id";
    public static final String ES_NEWS_FORUM_POST_SITE_ID = "site_id";
    public static final String ES_NEWS_FORUM_POST_PAGE_ID = "page_id";
    public static final String ES_NEWS_FORUM_POST_URL = "url";
    public static final String ES_NEWS_FORUM_POST_UPDATE_DATE = "update_date";
    public static final String ES_NEWS_FORUM_REVIEW_COUNT = "review_count";//评论数
    public static final String ES_NEWS_FORUM_VIEW_COUNT = "view_count";//阅读量


    //post hbase
    public static final String HBASE_NEWS_FORUM_POST_FAMLIY = "r";
    public static final String HBASE_NEWS_FORUM_POST_TITLE = "title";
    public static final String HBASE_NEWS_FORUM_POST_CONTENT = "content";
    public static final String HBASE_NEWS_FORUM_POST_VIEW_COUNT = "view_cnt";
    public static final String HBASE_NEWS_FORUM_POST_REVIEW_COUNT = "review_cnt";
//    public static final String HBASE_NEWS_FORUM_POST_CONTENT_LENGTH = "content_length";
    //public static final String HBASE_NEWS_FORUM_POST_MAIN_POST = "main_post";
    //public static final String HBASE_NEWS_FORUM_POST_MAIN_POST_LENGTH = "main_post_length";


    //comment hbase
    public static final String HBASE_NEWS_FORUM_COMMENT_FAMLIY = "r";
    public static final String HBASE_NEWS_FORUM_COMMENT_CONTENT = "content";
    public static final String HBASE_NEWS_FORUM_COMMENT_TITLE = "title";
    public static final String HBASE_NEWS_FORUM_COMMENT_CONTENT_LENGTH = "content_length";

    //comment es
    public static final String ES_NEWS_FORUM_COMMENT_CONTENT = "content";
    public static final String ES_NEWS_FORUM_COMMENT_PUBLISH_DATE = "publish_date";
    public static final String ES_NEWS_FORUM_COMMENT_PAGE_ID = "page_id";
    public static final String ES_NEWS_FORUM_COMMENT_URL = "url";
    public static final String ES_NEWS_FORUM_COMMENT_CAT_ID = "cat_id";

    public static final String ES_NEWS_FORUM_COMMENT_UPDATE_DATE = "update_date";
    //    public static final String ES_NEWS_FORUM_COMMENT_PUBLISH_TIME = "publish_time";
    public static final String ES_NEWS_FORUM_COMMENT_CONTENT_LENGTH = "content_length";
    public static final String ES_WEIBO_COMMENT_COMMENT_COMMENT_DATE = "comment_date";
    public static final String ES_WEIBO_COMMENT_COMMENT_COMMENT_TIME = "comment_time";

    //------------------news question answer---------------
    //post es

    public static final String ES_NEWS_QUESTION_POST_CONTENT = "content";
    public static final String ES_NEWS_QUESTION_POST_PUBLISH_TIME = "publish_time";
    public static final String ES_NEWS_QUESTION_POST_TITLE = "title";
    public static final String ES_NEWS_QUESTION_POST_UPDATE_DATE = "update_date";
    public static final String ES_NEWS_QUESTION_POST_CAT_ID = "cat_id";
    public static final String ES_NEWS_QUESTION_POST_LIKE = "like";
    public static final String ES_NEWS_QUESTION_PUBLISH_DATE_HOUR = "publish_date_our";
    public static final String ES_NEWS_QUESTION_IS_DIGEST = "is_digest";
    public static final String ES_NEWS_QUESTION_FINGERPRINT = "fingerprint";
    public static final String ES_NEWS_QUESTION_IS_AD = "is_ad";
    public static final String ES_NEWS_QUESTION_IS_RECOM = "is_recom";
    public static final String ES_NEWS_QUESTION_IS_HOT = "is_hot";
    public static final String ES_NEWS_QUESTION_CONTENT_LEN = "content_len";
    public static final String ES_NEWS_QUESTION_TASK_ID = "taskId";
    public static final String ES_NEWS_QUESTION_DISLIKE_CNT = "dislike_cnt";
    public static final String ES_NEWS_QUESTION_IS_TOP = "is_top";
    public static final String ES_NEWS_QUESTION_REVIEW_CNT = "review_cnt";
    public static final String ES_NEWS_QUESTION_VIEW_CNT = "view_cnt";


    //comment es


    public static final String ES_NEWS_ANSWER_COMMENT_CONTENT = "content";
    public static final String ES_NEWS_ANSWER_COMMENT_TITLE = "title";
    public static final String ES_NEWS_ANSWER_COMMENT_UPDATE_DATE = "update_date";
    public static final String ES_NEWS_ANSWER_COMMENT_COUNT = "comment_count";
    public static final String ES_NEWS_ANSWER_COMMENT_LIKE = "like";
    public static final String ES_NEWS_ANSWER_COMMENT_CNT = "comment_cnt";
    public static final String ES_NEWS_ANSWER_COMMENT_CAT_ID = "cat_id";
    public static final String ES_NEWS_ANSWER_COMMENT_FINGERPRINT = "fingerprint";
    public static final String ES_NEWS_ANSWER_COMMENT_IS_AD = "is_ad";
    public static final String ES_NEWS_ANSWER_COMMENT_CONTENT_LEN = "content_len";
    public static final String ES_NEWS_ANSWER_COMMENT_TASK_ID = "taskId";
    public static final String ES_NEWS_ANSWER_COMMENT_DISLIKE_CNT = "dislike_cnt";
    public static final String ES_NEWS_ANSWER_COMMENT_VIEW_CNT = "view_cnt";
    public static final String ES_NEWS_ANSWER_COMMENT_PARENT_POST_ID = "parent_post_id";

    //------------------wechat--------------------------
    public static final String ES_WECHAT_CONTENT = "content";
    public static final String ES_WECHAT_TITLE = "title";
    public static final String ES_WECHAT_PUBLISH_TIME = "publish_time";


    //------------------weibo comment------------------
    //comment hbase
    public static final String HBASE_WEIBO_COMMENT_COMMENT_FAMLIY = "r";
    public static final String HBASE_WEIBO_COMMENT_COMMENT_CONTENT = "content";

    public static final String ES_WEIBO_COMMENT_COMMENT_CONTENT = "content";


    //weibo hbase
//    public static final String HBASE_WEIBO_COMMENT_WEIBO_CONTENT = "content";
//    public static final String HBASE_WEIBO_COMMENT_WEIBO_RETWEET_CONTENT = "retweet_id";
//    public static final String HBASE_WEIBO_COMMENT_WEIBO_RETWEET_ID = "retweet_content";

    //------------------weibo------------------
    //weibo hbase
    public static final String HBASE_WEIBO_WEIBO_FAMILY = "r";
    public static final String HBASE_WEIBO_WEIBO_CONTENT = "content";
    public static final String HBASE_WEIBO_WEIBO_UID = "uid";


    public static final String HBASE_WEIBO_WEIBO_REPOST_MID = "rt_mid";
    public static final String HBASE_WEIBO_WEIBO_SRC_MID = "src_mid";


    //weibo weibo es
    public static final String ES_WEIBO_WEIBO_POST_TIME = "post_time";
    public static final String ES_WEIBO_WEIBO_POST_TIME_DATE = "post_time_date";
    public static final String ES_WEIBO_WEIBO_UID = "uid";
    public static final String ES_WEIBO_WEIBO_MID = "mid";
    public static final String ES_WEIBO_WEIBO_ID = "id";
    public static final String ES_WEIBO_WEIBO_PID = "pid";
    public static final String ES_WEIBO_WEIBO_MSG_TYPE = "msg_type";
    public static final String ES_WEIBO_WEIBO_RETWEET_ID = "retweet_id";
    public static final String ES_WEIBO_WEIBO_RETWEET_UID = "retweet_uid";
    public static final String ES_WEIBO_WEIBO_RETWEET_CONTENT = "retweet_content";
    public static final String ES_WEIBO_WEIBO_CONTENT = "content";
    public static final String ES_WEIBO_WEIBO_SELF_CONTENT = "self_content";

    //全量库字段
    public static final String ES_ALL_WEIBO_WEIBO_ATTITUDES = "attitudes";
    public static final String ES_ALL_WEIBO_WEIBO_REPOSTS = "reposts";
    public static final String ES_ALL_WEIBO_WEIBO_COMMENTS = "comments";
    public static final String ES_ALL_WEIBO_WEIBO_ATTITUDE_CNT = "attitudes_cnt";
    public static final String ES_ALL_WEIBO_WEIBO_REPOST_CNT = "reposts_cnt";
    public static final String ES_ALL_WEIBO_WEIBO_COMMENT_CNT = "comments_cnt";


    public static final String ES_WEIBO_WEIBO_COMMENT_MSG_TYPE = "weibo_msg_type";


    public static final String HBASE_WEIBO_WEIBO_QUALIFYS[] = new String[]{HBASE_WEIBO_WEIBO_CONTENT, HBASE_WEIBO_WEIBO_UID};

    public static final byte[] B_HBASE_WEIBO_WEIBO_FAMILY = Bytes.toBytes(HBASE_WEIBO_WEIBO_FAMILY);
    public static final byte[] B_HBASE_WEIBO_WEIBO_REPOST_MID = Bytes.toBytes(HBASE_WEIBO_WEIBO_REPOST_MID);
    public static final byte[] B_HBASE_WEIBO_WEIBO_SRC_MID = Bytes.toBytes(HBASE_WEIBO_WEIBO_SRC_MID);
    public static final byte[] B_HBASE_WEIBO_WEIBO_CONTENT = Bytes.toBytes(HBASE_WEIBO_WEIBO_CONTENT);


    public static final byte B_HBASE_WEIBO_WEIBO_QUALIFYS[][] = new byte[HBASE_WEIBO_WEIBO_QUALIFYS.length][];

    static {
        for (int i = 0; i < HBASE_WEIBO_WEIBO_QUALIFYS.length; i++) {
            B_HBASE_WEIBO_WEIBO_QUALIFYS[i] = Bytes.toBytes(HBASE_WEIBO_WEIBO_QUALIFYS[i]);
        }
    }


    //weibo user hbase
    public static final String HBASE_WEIBO_USER_FAMILY = "r";
    public static final String HBASE_WEIBO_USER_QUALIFY_NAME = "name";


    public static final String HBASE_WEIBO_USER_QUALIFYS[] = new String[]{HBASE_WEIBO_USER_QUALIFY_NAME};


    public static final byte[] B_HBASE_WEIBO_USER_FAMILY = Bytes.toBytes(HBASE_WEIBO_USER_FAMILY);
    public static final byte[] B_HBASE_WEIBO_USER_QUALIFY_NAME = Bytes.toBytes(HBASE_WEIBO_USER_QUALIFY_NAME);

    public static final byte B_HBASE_WEIBO_USER_QUALIFYS[][] = new byte[HBASE_WEIBO_USER_QUALIFYS.length][];

    static {
        for (int i = 0; i < HBASE_WEIBO_USER_QUALIFYS.length; i++) {
            B_HBASE_WEIBO_USER_QUALIFYS[i] = Bytes.toBytes(HBASE_WEIBO_USER_QUALIFYS[i]);
        }
    }


    //MIG es新增字段
    public static final String ES_FIELD_MIG_SENTIMENT = "mig_mention";

    //weibo weibo新增字段:username
    public static final String ES_FIELD_WEIBO_WEIBO_USER_NAME = "username";


    // ------------------ es writer ------------------
    public static final String ES_WEIBO_WRITER_INDEX = "dt-rhino-weibo-dbamp-index";
    public static final String ES_WEIBO_COMMENT_WRITER_INDEX = "dt-rhino-weibo-comment-dbamp-index";
    public static final String ES_NEWS_FORUM_WRITER_INDEX = "dt-rhino-newsforum-dbamp-index";
    public static final String ES_NEWS_QUESTION_ANSWER_WRITER_INDEX = "ds-rhino-qa-dbamp-v5";
    public static final String ES_WECHAT_INDEX = "";

    public static final String ES_WECHAT_PARENT_TYPE = "mp";
    public static final String ES_WECHAT_CHILD_TYPE = "wechat";
    public static final String ES_NEWS_QUESTION_PARENT_WRITE_TYPE = "question";
    public static final String ES_NEWS_ANSWER_CHILD_WRITE_TYPE = "answer";

    public static final String ES_WEIBO_PARENT_WRITE_TYPE = "user";
    public static final String ES_WEIBO_CHILD_WRITE_TYPE = "weibo";

    public static final String ES_WEIBO_COMMENT_PARENT_WRITE_TYPE = "weibo";
    public static final String ES_WEIBO_COMMENT_CHILD_WRITE_TYPE = "comment";

    public static final String ES_NEWS_FORUM_PARENT_WRITE_TYPE = "post";
    public static final String ES_NEWS_FORUM_CHILD_WRITE_TYPE = "comment";

    public static final String ES_FIELD_SENTIMENT = "sentiment";
    public static final String ES_FIELD_RETWEET_SENTIMENT = "retweet_sentiment";

    //ES中cat_id对应的含义
    public static final String ES_FIELDS_CAT_FORUM = "2";
    public static final String ES_FIELDS_CAT_NEWS = "3";
    public static final String ES_FIELDS_CAT_TIEBA = "8";
    public static final String ES_FIELDS_CAT_WENDA = "7";
    public static final String ES_FIELDS_CAT_BAIKE = "11";

    //贴吧site_id = 101537
    public static final String ES_FIELDS_SITE_TIEBA = "101537";


}
