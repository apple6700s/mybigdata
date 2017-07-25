package com.dt.mig.sync.extract;

import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.ds.dbamp.core.dao.es.YZDoc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 高频词提取
 * <p/>
 * Created by abel.chan on 16/12/21.
 */
public class HighFreqyWordExtract {

    private BackwardMatch backwardMatch;

    private WhiteList whiteListObj;

    private final static String FIELD_KEY = "keywords";

    private static HighFreqyWordExtract highFreqyWordExtract = null;

    public static HighFreqyWordExtract getInstance() {
        if (highFreqyWordExtract == null) {
            synchronized (HighFreqyWordExtract.class) {
                if (highFreqyWordExtract == null) {
                    highFreqyWordExtract = new HighFreqyWordExtract();
                }
            }
        }
        return highFreqyWordExtract;
    }


    public HighFreqyWordExtract() {
        this.backwardMatch = new BackwardMatch();
        this.whiteListObj = new WhiteList();
    }


    public void getHighFreqyWord(List<YZDoc> docs, String field) {
        if (docs != null) {
            for (YZDoc doc : docs) {
                getHighFreqyWord(doc, field);
            }
        }
    }

    public void getHighFreqyWord(YZDoc doc, String field) {
        if (doc != null && doc.containsKey(field) && doc.get(field) != null) {
            String content = doc.get(field).toString();
            String value = new String(content);
            Set<String> result = new HashSet<String>();
            Set<String> whiteList = backwardMatch.leftMax(value, this.whiteListObj);
            result.addAll(whiteList);
            //将content中的白名单剔除
            for (String whiteWord : whiteList) {
                value.replaceAll(whiteWord, "");
            }

            List<String> extractWords = KeywordsExtractor.getInstance().extract(value);
            if (extractWords != null && extractWords.size() > 0) {
                result.addAll(extractWords);
            }
            doc.put(FIELD_KEY, result);
        }
    }


}
