package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.weibo.analyz.SourceExtractor;
import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.component.SourcesComponentAnalyzer
 *
 * @author lhfcws
 * @since 2016/12/23
 */
public class SourcesComponentAnalyzer implements ComponentAnalyzer {
    @Override
    public List<String> getInputFields() {
        return Collections.singletonList("source");
    }

    public transient List<String> sourceList = new ArrayList<>();

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        if (valueIter == null)
            return null;

        setup();
        for (Object o : valueIter) {
            streamingAnalyz(o);
        }
        return cleanup();
    }

    @Override
    public void setup() {
        sourceList = new ArrayList<>();
    }

    @Override
    public void streamingAnalyz(Object o) {
        try {
            StrParams p = Util.transform(o);
            String sourceStr = p.get("sourceStr");
            if (sourceStr != null) {
                List<String> source = SourceExtractor.extractSources(sourceStr);
                if (source != null)
                    sourceList.addAll(source);
            }
        } catch (Exception ignore) {
        }
    }

    @Override
    public StrParams cleanup() {
        String r = null;
        if (!sourceList.isEmpty())
            r = StringUtils.join(sourceList, "|");
        sourceList.clear();
        return new StrParams("sources", r);
    }
}
