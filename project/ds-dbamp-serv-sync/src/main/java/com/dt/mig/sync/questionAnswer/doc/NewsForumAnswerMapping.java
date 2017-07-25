package com.dt.mig.sync.questionAnswer.doc;

import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * com.dt.mig.sync.questionAnswer.doc.NewsForumAnswerMapping
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public class NewsForumAnswerMapping {

    public static NewsForumAnswerMapping newsForumAnswerMapping;

    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{
            "parent_post_id", "parent_id",
            "id", "id",
            "url", "url",
            "content", "content",
            "keywords", "keywords",
            "author", "author",
            "like_cnt", "like_cnt",
            "is_robot", "is_robot",
            "publish_date", "publish_date",
            "publish_date_date", "publish_date_date",
            "publish_time", "publish_time",
            "sentiment", "sentiment",
            "domain", "domain",
            "title", "title",
            "update_date", "update_date",
            "review_cnt", "comment_cnt"
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

    private NewsForumAnswerMapping() {
    }

    public static NewsForumAnswerMapping getInstance() {
        if (newsForumAnswerMapping == null) {
            synchronized (NewsForumAnswerMapping.class) {
                if (newsForumAnswerMapping == null) {
                    newsForumAnswerMapping = new NewsForumAnswerMapping();
                }
            }
        }
        return newsForumAnswerMapping;
    }
}
