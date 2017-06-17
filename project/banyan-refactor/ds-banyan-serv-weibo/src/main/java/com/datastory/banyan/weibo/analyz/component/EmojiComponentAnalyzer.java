package com.datastory.banyan.weibo.analyz.component;

/**
 * IK
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class EmojiComponentAnalyzer extends SimpleMergeComponentAnalyzer {
    @Override
    public String getInField() {
        return "emoji";
    }

    @Override
    public String getOutField() {
        return "emojis";
    }
}
