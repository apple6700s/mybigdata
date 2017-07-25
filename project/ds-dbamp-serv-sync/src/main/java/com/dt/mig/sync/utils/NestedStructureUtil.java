package com.dt.mig.sync.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abel.chan on 16/11/21.
 */
public class NestedStructureUtil {
    public static void parseMigSentiment(List<Map<String, Object>> list, String mention, String sentiment, String sentence) {
        Map<String, Object> map = new HashMap<>();
        map.put("mention", mention);
        map.put("sentiment", sentiment);

        if (StringUtils.isNotEmpty(sentence) && sentence.length() > 32766 / 3) {
            map.put("sentence", sentence.substring(0, 32766 / 3));
        } else {
            map.put("sentence", sentence);
        }
        list.add(map);
    }

    public static void parseTrend(List<Map<String, Object>> list, String updateDate, Integer trendValue) {
        Map<String, Object> map = new HashMap<>();
        map.put("update_date", updateDate);
        map.put("count", trendValue);
        list.add(map);
    }

}
