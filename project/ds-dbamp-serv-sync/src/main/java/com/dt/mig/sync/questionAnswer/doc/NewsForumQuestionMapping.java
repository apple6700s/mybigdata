package com.dt.mig.sync.questionAnswer.doc;

import com.dt.mig.sync.newsforum.doc.NewsForumPostMapping;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * com.dt.mig.sync.questionAnswer.doc.NewsQuestionAnswerMapping
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public class NewsForumQuestionMapping {

    public static NewsForumQuestionMapping newsForumQuestionMapping;

    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{
            "id", "id",
            "url", "url",
            "content", "content",
            "sentiment", "sentiment",
            "is_robot", "is_robot",
            "author", "author",
            "like_cnt", "like_cnt",
            "site_id", "site_id",
            "site_name", "site_name",
            "publish_date", "publish_date",
            "publish_time", "publish_time",
            "publish_date_date", "publish_date_date",
            "domain", "domain",
            "keywords", "keywords",
            "update_date", "update_date"
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

    private NewsForumQuestionMapping() {

    }

    public static NewsForumQuestionMapping getInstance() {
        if (newsForumQuestionMapping == null) {
            synchronized (NewsForumPostMapping.class) {
                if (newsForumQuestionMapping == null) {
                    newsForumQuestionMapping = new NewsForumQuestionMapping();
                }
            }
        }
        return newsForumQuestionMapping;
    }
}
