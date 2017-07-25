package com.dt.mig.sync.utils;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.extract.HighFreqyWordExtract;
import com.dt.mig.sync.extract.MsgTypeExtractor;
import com.dt.mig.sync.extract.SelfContentExtractor;
import com.dt.mig.sync.sentiment.SentimentFactory;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.dt.mig.sync.words.FilterWords;
import com.dt.mig.sync.words.KeyWords;
import com.yeezhao.commons.util.Triple;

/**
 * Created by abel.chan on 17/4/5.
 */
public class WeiboUtil {

    //关键词加载
//    public final static KeyWords keyWords = KeyWords.getInstance();

    //过滤词加载
//    public final static FilterWords filterWords = FilterWords.getInstance();

//    public final static ISentiment sentimentAnaly = SentimentFactory.buildSentimentObj(SentimentFactory.SentimentType.LongText);//长文本情感分析

//    public final static HighFreqyWordExtract highFreqyWordExtract = new HighFreqyWordExtract();

    public static KeyWords getkeyWords() {
        return KeyWords.getInstance();
    }

    public static FilterWords getFilterWords() {
        return FilterWords.getInstance();
    }

    public static void setWeiboSelfContent(YZDoc doc) {
        if (doc != null) {
            doc.put(MigSyncConsts.ES_WEIBO_WEIBO_SELF_CONTENT, SelfContentExtractor.extract(BanyanTypeUtil.parseString(doc.get(MigSyncConsts.ES_WEIBO_WEIBO_SELF_CONTENT))));
        }
    }

    public static void setMid(YZDoc doc) {
        if (doc != null && doc.containsKey(MigSyncConsts.ES_WEIBO_WEIBO_ID)) {
            doc.put(MigSyncConsts.ES_WEIBO_WEIBO_MID, doc.get(MigSyncConsts.ES_WEIBO_WEIBO_ID));
        }
    }

    public static void setWeiboMsgType(YZDoc doc) {
        if (doc != null) {
            String rtMid = BanyanTypeUtil.parseString(doc.get(MigSyncConsts.ES_WEIBO_WEIBO_PID));
            String srcMid = BanyanTypeUtil.parseString(doc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID));
            String selfContent = BanyanTypeUtil.parseString(doc.get(MigSyncConsts.ES_WEIBO_WEIBO_SELF_CONTENT));
            String content = BanyanTypeUtil.parseString(doc.get(MigSyncConsts.ES_WEIBO_WEIBO_CONTENT));
            Short msgType = MsgTypeExtractor.analyz(srcMid, rtMid, selfContent, content);
            if (msgType != null) {
                doc.put(MigSyncConsts.ES_WEIBO_WEIBO_MSG_TYPE, msgType);
            }
        }
    }

    /**
     * 对doc的情感进行判断,根据keyword对每一个品牌,活动进行判断
     *
     * @param doc
     */
    public static void sentimentAnaly(YZDoc doc, String field, SentimentSourceType sentimentSourceType) {
        SentimentFactory.getLongTextInstance().analysis(getkeyWords(), field, MigSyncConsts.ES_FIELD_MIG_SENTIMENT, sentimentSourceType, doc);
    }

    /**
     * 替换content字段中的话题,并分析其情感
     *
     * @param doc
     */
    public static void removeTopicAnalySentiment(YZDoc doc, String fieldKey, SentimentSourceType sentimentSourceType) {
        removeTopicAnalySentiment(doc, fieldKey, MigSyncConsts.ES_FIELD_SENTIMENT, sentimentSourceType);
    }

    /**
     * 替换content字段中的话题,并分析其情感
     *
     * @param doc
     */
    public static void removeTopicAnalySentiment(YZDoc doc, String fieldKey, String sentimentKey, SentimentSourceType sentimentSourceType) {
        SentimentFactory.getLongTextInstance().removeTopicAnalysis(fieldKey, sentimentKey, sentimentSourceType, doc);
    }

    /**
     * 对doc的内容高频词提取,并写到keywords里面
     *
     * @param doc
     */
    public static void extractHighFreqyWord(YZDoc doc, String field) {
        HighFreqyWordExtract.getInstance().getHighFreqyWord(doc, field);
    }

    public static Triple<Integer, Integer, Integer> getTrendValue(String value) {
        try {
            String[] split = value.split("\\|");
            if (split != null && split.length == 3) {

                return new Triple<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2]));
            } else {
                System.out.println("ERROR TREND VALUE:" + value);
            }
        } catch (Exception e) {
        }
        return new Triple<>(0, 0, 0);
    }

    public static void main(String[] args) {
        getTrendValue("11|0|13");
    }
}
