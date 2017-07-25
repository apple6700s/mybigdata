package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.component.FreqMergeComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/13
 */
public abstract class FreqMergeComponentAnalyzer extends SimpleMergeComponentAnalyzer {

    public abstract int getTopNSize();

    public transient FreqDist<String> freqDist = new FreqDist<>();
    public transient StrParams ret = new StrParams();

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
        freqDist = new FreqDist<>();
        ret = new StrParams();
    }

    @Override
    public void streamingAnalyz(Object o) {
        Map<String, String> mp = Util.transform(o);
        String kw = mp.get(getInField());
        if (!StringUtil.isNullOrEmpty(kw)) {
            String[] kws = kw.split(StringUtil.STR_DELIMIT_1ST);
            for (String k : kws) {
                freqDist.inc(k);
            }
        }
    }

    @Override
    public StrParams cleanup() {
        if (!freqDist.isEmpty()) {
            int size = freqDist.size();
            String res;
            if (size > getTopNSize()) {
                List<Map.Entry<String, Integer>> list = freqDist.sortValues(false);
                freqDist.clear();
                StringBuilder sb = new StringBuilder();
                int i = 0;
                for (Map.Entry<String, Integer> e : list) {
                    sb.append(e.getKey());
                    if (i < getTopNSize())
                        sb.append("|");
                    else if (i > getTopNSize())
                        break;
                }
                list.clear();
                res = sb.toString();
            } else {
                res = StringUtils.join(freqDist.keySet(), "|");
            }

            ret.put(getOutField(), res);
        }
        StrParams p = new StrParams(ret);
        ret.clear();
        return p;
    }
}
