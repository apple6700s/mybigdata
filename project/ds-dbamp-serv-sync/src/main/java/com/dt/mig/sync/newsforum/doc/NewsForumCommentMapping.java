package com.dt.mig.sync.newsforum.doc;

import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Created by abel.chan on 17/4/11.
 */
public class NewsForumCommentMapping {
    private static NewsForumCommentMapping newsForumCommentMapping;


    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{
            "review_cnt", "review_count",
            "parent_post_id", "parent_id",
            "publish_time", "publish_date"
    });

    /**
     * 获取新的映射key,如果key不存在,则返回自己。
     *
     * @param key
     * @return
     */
    public String getNewKey(String key) {
        if (StringUtils.isEmpty(key) || !mapping.containsKey(key)) return key;
        return mapping.get(key);
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    private NewsForumCommentMapping() {

    }

    public static NewsForumCommentMapping getInstance() {
        if (newsForumCommentMapping == null) {
            synchronized (NewsForumCommentMapping.class) {
                if (newsForumCommentMapping == null) {
                    newsForumCommentMapping = new NewsForumCommentMapping();
                }
            }
        }
        return newsForumCommentMapping;
    }
}
