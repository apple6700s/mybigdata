package com.datastory.banyan.wechat.analyz;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

/**
 * com.datastory.banyan.wechat.analyz.WxArticleIdGen
 *
 * @author lhfcws
 * @since 2016/12/22
 */
public class WxArticleIdGen {
    public static String genId(String url) {
        if (url == null) return null;

        String tmpUrl = StringUtils.substringAfter(url, "?");
        String[] args = tmpUrl.split("&");
        HashMap<String, String> articleKey = new HashMap<>();
        for (String str : args) {
            if (!str.contains("=")) continue;
            String name = StringUtils.substringBefore(str, "=");
            String value = StringUtils.substringAfter(str, "=");
            articleKey.put(name, value);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(articleKey.get("__biz")).append(":")
                .append(articleKey.get("mid")).append(":")
                .append(articleKey.get("idx")).append(":")
                .append(articleKey.get("sn"));
        return sb.toString();
    }
}
