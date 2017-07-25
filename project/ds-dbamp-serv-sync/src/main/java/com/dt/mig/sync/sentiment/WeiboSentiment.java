package com.dt.mig.sync.sentiment;

import com.yeezhao.hornbill.analyz.algo.sentiment.tweets.MigSntmntClassifier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Created by abel.chan on 16/11/21.
 */
public class WeiboSentiment {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboSentiment.class);

    private MigSntmntClassifier sentimentExtractor = null;


    public void setUp() {
        try {
            sentimentExtractor = MigSntmntClassifier.getInstance();
            //sentimentExtractor = new MigSntmntClassifier();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    /**
     * 分析文本的情感
     *
     * @param summaryContent 关键词提取的短句集合
     * @return
     */
    public String senAnaly(String summaryContent) {
        try {
            if (sentimentExtractor != null) {
                if (StringUtils.isEmpty(summaryContent)) {
                    return "0";
                }
                //判断是否是QQ浏览器推送新闻
                if (checkIsQQBrowserNews(summaryContent)) {
                    //来自QQ浏览器或手机QQ浏览器的推送新闻。
                    return "0";
                }

                String keyWordSentiment = sentimentExtractor.classify(removeExpression(summaryContent));
                if (StringUtils.isEmpty(keyWordSentiment)) {
                    return "0";
                }
                return keyWordSentiment;
            }
            throw new Exception("sentimentExtractor is null!");
        } catch (Exception e) {
            LOG.error("error text :" + summaryContent + "\n  " + e.getMessage(), e);
        }
        return "0";
    }


    //提取表情的字符串
    public final static String REGEX_EXPRESSION = "\\[[\\u4e00-\\u9fa5a-zA-Z0-9]+\\]";

    public static String removeExpression(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        return new String(str).replaceAll(REGEX_EXPRESSION, "");
    }

    /**
     * weibo类型时需要先检查其是否为转发贴,若是转发贴,则看其是否是QQ浏览器的推送新闻。
     *
     * @param retweetContentObj
     * @param summaryContent
     * @return
     */
    public String senAnalyWithWeibo(Object retweetContentObj, String summaryContent) {
        if (retweetContentObj == null) {
            return senAnaly(summaryContent);
        }
        String retweetContent = retweetContentObj.toString();
        //判断是否是QQ浏览器推送新闻
        if (StringUtils.isNotEmpty(retweetContent) && checkIsQQBrowserNews(retweetContent)) {
            //来自QQ浏览器或手机QQ浏览器的推送新闻。
            return "0";
        }

        return senAnaly(summaryContent);

    }

    public final static Pattern PATTERN_REGEX_QQBROWSER_NEWS = Pattern.compile("(@QQ浏览器|@手机QQ浏览器)\\s+(https?://(w{3}\\.)?)?\\w+\\.\\w+(\\.[a-zA-Z]+)*(:\\d{1,5})?(/\\w*)*(\\??(.+=.*)?(&.+=.*)?)?");

    public static boolean checkIsQQBrowserNews(String str) {
        if (StringUtils.isEmpty(str)) {
            return false;
        }
        return PATTERN_REGEX_QQBROWSER_NEWS.matcher(str.trim()).find();
    }


    /**
     * 去除文本中满足正则的内容
     *
     * @param text
     * @return
     */
    public String removeSentenceWithRegex(String text) {
        if (text == null) return null;
        return text.replaceAll("#[^\\#|.]+#", " ");
    }


    public String getWeiboSentiment(String content, String retweet_content) {
        try {
            if (StringUtils.isNotEmpty(content)) {
                return senAnalyWithWeibo(retweet_content, removeSentenceWithRegex(content));
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
        return "0";

    }

    public static void main(String[] args) {
        //String content = "电脑管家|电脑安全管家|腾讯电脑安全管家";
        String content = "求解释，怎么把微信储存空间清理干净？[摊手]@腾讯微信团队 @腾讯公司 @腾讯电脑管家 @腾讯QQ产品团队 @腾讯新闻客户端 @腾讯微信读书 ?";
        String retweet_content = "";
        WeiboSentiment weiboSentiment = new WeiboSentiment();
        weiboSentiment.setUp();
        String sentiment = weiboSentiment.getWeiboSentiment(content, retweet_content);
        System.out.println(sentiment);
    }

}
