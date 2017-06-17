package com.datastory.banyan.weibo.analyz.user_component;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.weibo.doc.scavenger.HBaseParams2UserInfoDocMapper;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.hornbill.algo.astrotufing.classifier.AstrotufingClassfier;
import com.yeezhao.hornbill.analyz.common.entity.UserAnalyzBasicInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.user_component.UTypeUserAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public class UTypeUserAnalyzer implements UserAnalyzer {

    @Override
    public List<String> getInputFields() {
        return Arrays.asList(
                "verified_type", "name", "url", "weihao", "domain",
                "wb_cnt", "fav_cnt", "fans_cnt", "follow_cnt", "bi_follow_cnt"
        );
    }

    @Override
    public String getOutputTable() {
        return Tables.ANALYZ_USER_TBL;
    }

    @Override
    public StrParams analyz(Map<String, String> user) {
        UserAnalyzBasicInfo info = new HBaseParams2UserInfoDocMapper(user).map();
        try {
            Pair<Integer, Integer> pair = AstrotufingClassfier.classifySimpleWithStatus(info);
            if (pair.second < 2)
                return new StrParams("user_type_a", pair.first + "$" + pair.second);
        } catch (Exception e) {
            ErrorUtil._LOG.error(e.getMessage(), e);
        }
        return new StrParams();
    }

}
