package com.dt.mig.sync.utils;

import com.ds.dbamp.core.dao.es.YZDoc;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by abel.chan on 17/2/10.
 */
public class SentimentUtil {
    private final static String SENTIMENT_KEY = "sentiment";

    /**
     * 如果情感字段为空的话,设置为中性。
     *
     * @param doc
     */
    public static void checkAndSetSentiment(YZDoc doc) {
        try {
            if (doc == null) return;
            if (doc.containsKey(SENTIMENT_KEY) && doc.get(SENTIMENT_KEY) != null && StringUtils.isNotEmpty(doc.get(SENTIMENT_KEY).toString().trim())) {
                return;
            }
            //排除异常值。
            Short.parseShort(doc.get(SENTIMENT_KEY).toString());
        } catch (Exception ex) {
            //设置一个默认的key
            doc.put(SENTIMENT_KEY, getDefultSentiment());
        }
    }


    private static short getDefultSentiment() {
        return 0;
    }
}
