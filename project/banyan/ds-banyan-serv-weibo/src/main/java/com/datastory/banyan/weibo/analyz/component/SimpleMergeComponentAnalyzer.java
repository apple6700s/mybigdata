package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.component.SimpleMergeComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public abstract class SimpleMergeComponentAnalyzer implements ComponentAnalyzer {
    public abstract String getInField();
    public abstract String getOutField();

    public transient HashSet<String> set = new HashSet<>();
    public transient StrParams ret = new StrParams();

    @Override
    public List<String> getInputFields() {
        return Arrays.asList(getInField());
    }

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        setup();
        for (Object o : valueIter) {
           streamingAnalyz(o);
        }
        return cleanup();
    }

    @Override
    public void setup() {
        set = new HashSet<>();
        ret = new StrParams();
    }

    @Override
    public void streamingAnalyz(Object o) {
        Map<String, String> mp = Util.transform(o);
        String kw = mp.get(getInField());
        if (!StringUtil.isNullOrEmpty(kw)) {
            String[] kws = kw.split(StringUtil.STR_DELIMIT_1ST);
            for (String k : kws) {
                set.add(k);
            }
        }
    }

    @Override
    public StrParams cleanup() {
        if (!set.isEmpty())
            ret.put(getOutField(), StringUtils.join(set, "|"));
        set.clear();
        StrParams p = new StrParams(ret);
        ret.clear();
        return p;
    }
}
