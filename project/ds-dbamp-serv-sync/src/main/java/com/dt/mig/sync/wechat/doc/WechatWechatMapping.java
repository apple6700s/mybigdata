package com.dt.mig.sync.wechat.doc;

import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * com.dt.mig.sync.wechat.doc.WechatWechatMapping
 *
 * @author zhaozhen
 * @since 2017/7/17
 */
public class WechatWechatMapping {

    public static WechatWechatMapping wechatWechatMapping;

    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{
            "id", "id",
            "mid", "mid",
            "article_id", "article_id",
            "biz", "biz",
            "url", "url",
            "sentiment", "sentiment",
            "is_ad", "is_ad",
            "author", "author",
            "wx_author", "wxauthor",
            "publish_date_hour", "publish_date_hour",
            "publish_time", "publish_time",
            "publish_date_date", "publish_date_date",
            "title", "title",
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

    private WechatWechatMapping() {

    }

    public static WechatWechatMapping getInstance() {
        if (wechatWechatMapping == null) {
            synchronized (WechatWechatMapping.class) {
                if (wechatWechatMapping == null) {
                    wechatWechatMapping = new WechatWechatMapping();
                }
            }
        }
        return wechatWechatMapping;
    }
}
