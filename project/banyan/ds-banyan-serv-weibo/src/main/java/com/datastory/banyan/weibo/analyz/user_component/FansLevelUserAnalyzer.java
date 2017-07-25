package com.datastory.banyan.weibo.analyz.user_component;

import com.datastory.banyan.weibo.analyz.FansLevelTranslator;
import com.datastory.banyan.base.Tables;
import com.yeezhao.commons.util.Entity.StrParams;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.user_component.FansLevelUserAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/17
 */
public class FansLevelUserAnalyzer implements UserAnalyzer {
    @Override
    public List<String> getInputFields() {
        return Arrays.asList("fans_cnt");
    }

    @Override
    public String getOutputTable() {
        return Tables.table(Tables.PH_WBUSER_TBL);
    }

    @Override
    public StrParams analyz(Map<String, String> user) {
        String fansCnt = user.get("fans_cnt");
        int fansLevel = FansLevelTranslator.getNewFollowersCountRank(fansCnt);
        return new StrParams("fans_level", fansLevel + "");
    }
}
