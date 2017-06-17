package com.datastory.banyan.weibo.analyz.component;

import com.yeezhao.commons.util.Entity.StrParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.component.HalfMonthComponentAnalyzer
 * 需要每半个月更新一次的字段，这是一个聚合型的component analyzer
 *
 * @author lhfcws
 * @since 2017/1/13
 */
public class HalfMonthComponentAnalyzer implements ComponentAnalyzer {
    private List<ComponentAnalyzer> halfMonthAnalyzers = new ArrayList<>();

    public HalfMonthComponentAnalyzer() {
        halfMonthAnalyzers.addAll(Arrays.asList(
                new EmojiComponentAnalyzer(),
                new FootprintComponentAnalyzer(),
                new KeywordsComponentAnalyzer(),
                new SourcesComponentAnalyzer(),
                new TopicsComponentAnalyzer(),
                new RecentActivenessComponentAnalyzer()
        ));
        System.out.println("Init halfMonthAnalyzers : " + halfMonthAnalyzers);
    }

    @Override
    public List<String> getInputFields() {
        List<String> inputFields = new ArrayList<>();
        for (ComponentAnalyzer analyzer : halfMonthAnalyzers) {
            inputFields.addAll(analyzer.getInputFields());
        }
        return inputFields;
    }

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
        for (ComponentAnalyzer analyzer : halfMonthAnalyzers) {
            analyzer.setup();
        }
    }

    @Override
    public void streamingAnalyz(Object obj) {
        if (obj == null)
            return;
        for (ComponentAnalyzer analyzer : halfMonthAnalyzers) {
            analyzer.streamingAnalyz(obj);
        }
    }

    @Override
    public StrParams cleanup() {
        StrParams ret = new StrParams();
        for (ComponentAnalyzer analyzer : halfMonthAnalyzers) {
            ret.putAll(analyzer.cleanup());
        }
        return ret;
    }
}
