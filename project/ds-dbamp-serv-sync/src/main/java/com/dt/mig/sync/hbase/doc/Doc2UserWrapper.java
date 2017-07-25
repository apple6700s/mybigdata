package com.dt.mig.sync.hbase.doc;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.DateUtils;
import com.dt.mig.sync.utils.SourceExtractorUtil;
import com.google.gson.Gson;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abel.chan on 17/02/21.
 */
public class Doc2UserWrapper extends DocWrapper<YZDoc> {

    private static final Logger LOG = LoggerFactory.getLogger(Doc2UserWrapper.class);

    private static final Gson gson = new Gson();

    public static Map<String, String> mapping = new HashMap<String, String>() {
    };

    static {
        mapping.put("uid", "id");
        mapping.put("name", "nickname");
        mapping.put("birthyear", "birthyear");
        mapping.put("birthdate", "birthdate");
        mapping.put("constellation", "constellation");
        mapping.put("user_type", "user_type");
        mapping.put("verified_type", "verified_type");
        mapping.put("vtype", "vtype");
        mapping.put("gender", "gender");
        mapping.put("fans_cnt", "follower_count");
        mapping.put("follow_cnt", "friend_count");
        mapping.put("wb_cnt", "weibo_count");
        mapping.put("fav_cnt", "favorite_count");
        mapping.put("province", "province");
        mapping.put("city", "city");
        mapping.put("city_level", "city_type");
        mapping.put("meta_group", "meta_group");
        mapping.put("sources", "sources");
        mapping.put("company", "company");
        mapping.put("school", "school");
        mapping.put("activeness", "activeness");
        mapping.put("fans_level", "fans_level");
        mapping.put("topics", "topics");
        mapping.put("desc", "description");
        mapping.put("publish_date", "create_date");
    }

    public Doc2UserWrapper(Params doc) {
        super(doc);
    }

    public YZDoc objWrapper() {
        YZDoc yzDoc = new YZDoc();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.isNotEmpty(key) && value != null) {
                if (mapping.containsKey(key)) {
                    if (key.equals("user_type") || key.equals("fans_level") || key.equals("vtype") || key.equals("gender")) {
                        yzDoc.put(mapping.get(key), BanyanTypeUtil.parseShort(value));
                    } else if (key.equals("meta_group")) {
                        List<String> list = BanyanTypeUtil.yzStr2List(value.toString());
                        if (list != null && list.size() > 0) {
                            // 先拿150个，太大了。。。
                            if (list.size() > 150) list = list.subList(1, 150);
                            else list = list.subList(1, list.size());
//                            list.remove(0);
                            yzDoc.put(mapping.get(key), list);
                        }
                    } else if (key.equals("sources")) {
                        List<String> list = SourceExtractorUtil.extractSources(value.toString());
                        if (list != null && list.size() > 0) {
                            yzDoc.put(mapping.get(key), list);
                        }
                    } else if (key.equals("school") || key.equals("topics") || key.equals("company")) {
                        List<String> list = BanyanTypeUtil.yzStr2List(value.toString());
                        if (list != null && list.size() > 0) {
                            if (list.size() > 150) list = list.subList(0, 150);
                            else list = list.subList(0, list.size());
                            yzDoc.put(mapping.get(key), list);
                        }
                    } else {
                        yzDoc.put(mapping.get(key), value);
                    }
                }
            }
        }
        yzDoc.put("update_date", DateUtils.getCurrentTimeStr());
        return yzDoc;
    }

    public static void main(String[] args) {
        Result result = new Result();
        Result2DocMapper doc = new Result2DocMapper(result, "r".getBytes());
        Doc2UserWrapper wrapper = new Doc2UserWrapper(doc.map());
        YZDoc yzDoc = wrapper.objWrapper();
    }
}
