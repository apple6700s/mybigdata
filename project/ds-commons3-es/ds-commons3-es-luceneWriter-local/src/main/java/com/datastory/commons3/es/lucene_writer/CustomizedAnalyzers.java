package com.datastory.commons3.es.lucene_writer;

import org.elasticsearch.index.analysis.AnalysisModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * com.datastory.commons3.es.lucene_writer.CustomizedAnalyzers
 *
 * @author lhfcws
 * @since 2017/6/6
 */
public class CustomizedAnalyzers {
    private static List<AnalysisModule.AnalysisBinderProcessor> analyzerProcessors = new ArrayList<>();

    public static void add(AnalysisModule.AnalysisBinderProcessor... processors) {
        analyzerProcessors.addAll(Arrays.asList(processors));
    }

    public static List<AnalysisModule.AnalysisBinderProcessor> getCustomizedAnalyzers() {
        return analyzerProcessors;
    }
}
