package com.dt.mig.sync.newsforum.doc;

import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Created by abel.chan on 17/4/11.
 */
public class NewsForumPostMapping {
    private static NewsForumPostMapping newsForumPostMapping;


    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{
            "all_content", "content",
            "all_content_len", "content_length",
            "content", "main_post",
            "content_len", "main_post_length",
            "view_cnt", "view_count",
            "review_cnt", "review_count",
            "publish_time", "publish_date",
            "taskId", "task_id",
            "is_recom", "is_recomment",
            "site_name", "forum_name",
            "source", "news_source"
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

    private NewsForumPostMapping() {

    }

    public static NewsForumPostMapping getInstance() {
        if (newsForumPostMapping == null) {
            synchronized (NewsForumPostMapping.class) {
                if (newsForumPostMapping == null) {
                    newsForumPostMapping = new NewsForumPostMapping();
                }
            }
        }
        return newsForumPostMapping;
    }
}
