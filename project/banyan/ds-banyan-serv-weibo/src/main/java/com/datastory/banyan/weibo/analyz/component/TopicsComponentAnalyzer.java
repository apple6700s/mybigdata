package com.datastory.banyan.weibo.analyz.component;


/**
 * com.datastory.banyan.weibo.analyz.component.TopicsComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class TopicsComponentAnalyzer extends SimpleMergeComponentAnalyzer {

    @Override
    public String getInField() {
        return "topics";
    }

    @Override
    public String getOutField() {
        return "topics";
    }
}
