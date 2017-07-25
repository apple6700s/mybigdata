package com.datastory.banyan.wechat.analyz;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * com.datastory.banyan.wechat.analyz.OpenIdExtractor
 *
 * @author lhfcws
 * @since 2016/12/21
 */
public class OpenIdExtractor {
    static Pattern P_OPENID = Pattern.compile("[0-9a-zA-Z_-]{20,}");

    public static boolean isOpenID(String kw) {
        if (StringUtils.isEmpty(kw))
            return false;
        return (P_OPENID.matcher(kw).matches());
    }
}
