package com.datastory.banyan.weibo.analyz;

import org.apache.commons.lang3.StringUtils;

/**
 * com.datastory.banyan.weibo.analyz.MsgTypeAnalyzer
 *
 * @author sezina
 * @since 2016/11/9
 */
public class MsgTypeAnalyzer {
    /**
     * @param srcMid   博文最原始的id
     * @param rtMid    pid
     * @param selfText 自身发布的内容
     * @param text     全部内容,包括自身发布的内容
     * @return -1 未知,0 原贴, 1 一层转发不带文字 2 一层转发待文字
     */
    public static short analyz(String srcMid, String rtMid, String selfText, String text) {
        if (StringUtils.isEmpty(text))
            return -1;
        if (StringUtils.isEmpty(srcMid))
            return 0;

        if ((StringUtils.isEmpty(rtMid) && !text.contains("//@"))
                || (StringUtils.isNotEmpty(rtMid) && rtMid.equals(srcMid))) {
            if (StringUtils.isNotEmpty(selfText)) {
                return 2;
            } else {
                return 1;
            }

        } else {
            return 3;
        }
    }
}
