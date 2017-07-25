package com.dt.mig.sync.hbase.doc;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.google.gson.Gson;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by abel.chan on 17/02/21.
 */
public class Doc2WeiboWrapper extends DocWrapper<YZDoc> {

    private static final Logger LOG = LoggerFactory.getLogger(Doc2WeiboWrapper.class);

    private static final Gson gson = new Gson();

    public Doc2WeiboWrapper(Params params) {
        super(params);
    }

    public static Map<String, String> mapping = new HashMap<String, String>() {
    };

    static {
        mapping.put("mid", "id");
        mapping.put("rt_mid", "pid");
        mapping.put("content", "content");
        mapping.put("self_content", "self_content");
        mapping.put("source", "source");
        mapping.put("topics", "topics");

        mapping.put("msg_type", "msg_type");
        mapping.put("src_mid", "retweet_id");
        mapping.put("src_content", "retweet_content");


        mapping.put("reposts_cnt", "reposts_count");
        //mapping.put("","reposts");
        mapping.put("comments_cnt", "comments_count");
        //mapping.put("","comments");
        mapping.put("attitudes_cnt", "attitudes_count");
        //mapping.put("","attitudes");

//        mapping.put("", "interactions_count");
//        mapping.put("", "repost_comment_count");

        mapping.put("sentiment", "sentiment");
        mapping.put("keywords", "keywords");
        mapping.put("username", "username");

        //根据publish_date
//        mapping.put("", "post_time");
//        mapping.put("", "post_time_hour");
//        mapping.put("", "post_time_date");

        //需要后续获取
//        mapping.put("", "username");
//        mapping.put("mig_mention", "mig_mention");
    }

    public YZDoc objWrapper() {
        YZDoc yzDoc = new YZDoc();

        Params params = this.params;

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.isNotEmpty(key) && value != null) {
                if (!key.equals("publish_date")) {
                    if (mapping.containsKey(key)) {
                        if (key.equals("msg_type") || key.equals("sentiment")) {
                            yzDoc.put(mapping.get(key), BanyanTypeUtil.parseShort(value));
                        } else {
                            yzDoc.put(mapping.get(key), value);
                        }
                    }
                } else {
                    if (value.toString().length() == 14) {
                        Date date = DATE_TIME_FORMATTER.parseDateTime(value.toString()).toDate();
                        yzDoc.put("post_time", date.getTime());
                        yzDoc.put("post_time_hour", value.toString().substring(0, 10));
                        yzDoc.put("post_time_date", value.toString().substring(0, 8));
                    }
                }

            }
        }
        if (params.containsKey("reposts_cnt") && params.containsKey("comments_cnt") && params.containsKey("attitudes_cnt")) {
            yzDoc.put("interactions_count", BanyanTypeUtil.parseIntForce(params.getString("reposts_cnt")) + BanyanTypeUtil.parseIntForce(params.getString("comments_cnt")) + BanyanTypeUtil.parseIntForce(params.getString("attitudes_cnt")));
        }

        if (params.containsKey("reposts_cnt") && params.containsKey("comments_cnt")) {
            yzDoc.put("repost_comment_count", BanyanTypeUtil.parseIntForce(params.getString("reposts_cnt")) + BanyanTypeUtil.parseIntForce(params.getString(params.getString("comments_cnt"))));

        }


        return yzDoc;
    }


    public static void main(String[] args) {
        Result result = new Result();
        Result2DocMapper doc = new Result2DocMapper(result, "r".getBytes());
        Doc2WeiboWrapper wrapper = new Doc2WeiboWrapper(doc.map());
        YZDoc yzDoc = wrapper.objWrapper();
    }
}
