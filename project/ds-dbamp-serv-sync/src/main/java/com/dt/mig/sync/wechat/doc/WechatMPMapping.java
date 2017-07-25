package com.dt.mig.sync.wechat.doc;

import com.dt.mig.sync.newsforum.doc.NewsForumPostMapping;
import com.dt.mig.sync.questionAnswer.doc.NewsForumQuestionMapping;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * com.dt.mig.sync.wechat.doc.WechatMPMapping
 *
 * @author zhaozhen
 * @since 2017/7/17
 */
public class WechatMPMapping {

    public static WechatMPMapping wechatMPMapping;

    private Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(new String[]{

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

    private WechatMPMapping() {

    }

    public static WechatMPMapping getInstance() {
        if (wechatMPMapping == null) {
            synchronized (WechatMPMapping.class) {
                if (wechatMPMapping == null) {
                    wechatMPMapping = new WechatMPMapping();
                }
            }
        }
        return wechatMPMapping;
    }
}
