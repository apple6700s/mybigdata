package com.datastory.banyan.weibo.analyz.component;

/**
 * com.datastory.banyan.weibo.analyz.component.FootprintComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class FootprintComponentAnalyzer extends SimpleMergeComponentAnalyzer {
    @Override
    public String getInField() {
        return "footprint";
    }

    @Override
    public String getOutField() {
        return "footprints";
    }
}
