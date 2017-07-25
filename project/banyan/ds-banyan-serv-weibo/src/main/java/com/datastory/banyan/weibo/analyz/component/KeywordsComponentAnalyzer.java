package com.datastory.banyan.weibo.analyz.component;


import com.datastory.banyan.base.RhinoETLConsts;

/**
 * com.datastory.banyan.weibo.analyz.component.KeywordsComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class KeywordsComponentAnalyzer extends FreqMergeComponentAnalyzer {
    @Override
    public String getInField() {
        return "keywords";
    }

    @Override
    public String getOutField() {
        return "keywords";
    }

    @Override
    public int getTopNSize() {
        return RhinoETLConsts.MAX_ANALYZ_LEN;
    }
}
