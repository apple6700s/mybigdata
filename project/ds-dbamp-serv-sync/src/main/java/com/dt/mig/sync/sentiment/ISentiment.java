package com.dt.mig.sync.sentiment;


import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.words.IWords;

import java.util.List;


/**
 * Created by abel.chan on 16/11/21.
 */
public interface ISentiment {
    /**
     * 初始化参数
     */
    public void setUp();

    /**
     * 分析文本的情感
     *
     * @param srcContent          文章的源内容
     * @param summaryContent      关键词提取的短句集合
     * @param sentimentSourceType 语句的来源,微博,长文本,评论
     * @return
     */
    public String senAnaly(String srcContent, String summaryContent, SentimentSourceType sentimentSourceType);

    /**
     * 长文本提取满足关键词的句子,并合并
     *
     * @param text
     * @param keywords
     * @return
     */
    public String getSentenceSegWithKey(String text, String keywords);


    /**
     * 去除文本中满足正则的内容
     *
     * @param text
     * @return
     */
    public String removeSentenceWithRegex(String text);

    /**
     * 对doc的情感进行判断,并写到docscam随机,根据keyword对每一个品牌,活动进行判断
     *
     * @param keyWords
     * @param contentField
     * @param sentimentField
     * @param docs
     */
    public void analysis(IWords keyWords, String contentField, String sentimentField, SentimentSourceType sentimentSourceType, List<YZDoc> docs);

    /**
     * 对doc的情感进行判断,并写到docscam随机,根据keyword对每一个品牌,活动进行判断
     *
     * @param keyWords
     * @param contentField
     * @param sentimentField
     * @param doc
     */
    public void analysis(IWords keyWords, String contentField, String sentimentField, SentimentSourceType sentimentSourceType, YZDoc doc);

    /**
     * 替换content字段中的话题,并分析其情感
     *
     * @param docs
     */
    public void removeTopicAnalysis(String contentField, String sentimentField, SentimentSourceType sentimentSourceType, List<YZDoc> docs);


    public void removeTopicAnalysis(String contentField, String sentimentField, SentimentSourceType sentimentSourceType, YZDoc doc);
}
