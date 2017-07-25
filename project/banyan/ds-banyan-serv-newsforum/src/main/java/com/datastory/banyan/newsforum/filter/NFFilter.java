package com.datastory.banyan.newsforum.filter;

import com.datastory.banyan.filter.AbstractFilter;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.newsforum.filter.NFFilter
 *
 * @author lhfcws
 * @since 16/11/25
 */

public class NFFilter extends AbstractFilter {
    @Override
    public boolean filter(Object obj) throws Throwable {
        Params p = (Params) obj;
        if (p == null || p.isEmpty())
            return true;
        if (!DateUtils.validateDatetime(p.getString("publish_date")))
            return true;
        if (!NewsForumAnalyzer.getInstance().isMainPost(p)
                && BanyanTypeUtil.len(p.getString("content")) > 1000)
            return true;
        return false;
    }
}
