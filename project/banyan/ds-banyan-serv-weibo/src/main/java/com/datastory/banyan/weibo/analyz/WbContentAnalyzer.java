package com.datastory.banyan.weibo.analyz;


import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.util.WeiboUtils;
import com.datatub.scavenger.extractor.AdDetection;
import com.datatub.scavenger.extractor.CommonRegexExtractor;
import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.datatub.scavenger.extractor.MigSentimentExtractor;
import com.datatub.scavenger.util.InitialContext;
import com.google.common.base.Joiner;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * com.datatub.banyan.wbcontent.analyz.WbContentAnalyzer
 *
 * @author lhfcws
 * @since 2016/10/23
 */
public class WbContentAnalyzer {
    protected static final int KEYWORD_AMOUNT = 3;
    protected static Logger LOG = Logger.getLogger(WbContentAnalyzer.class);

    final AccuStat aRegexExtractorStat = new AccuStat();
    final AccuStat aAdDetectionStat = new AccuStat();
    final AccuStat aSentimentStat = new AccuStat();
    final AccuStat aKeyWordsStat = new AccuStat();
    final AccuStat aUserTypeStat = new AccuStat();

    private WbContentAnalyzer() {
        InitialContext.init();
    }

    private static volatile WbContentAnalyzer _singleton = null;

    public static WbContentAnalyzer getInstance() {
        if (_singleton == null)
            synchronized (WbContentAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new WbContentAnalyzer();
                }
            }
        return _singleton;
    }

    public static String removeNonBmpUnicode(String str) {
        if (str == null) {
            return null;
        }
        str = str.replaceAll("[^\\u0000-\\uFFFF]", "");
        return str;
    }

    public Params analyz(Params weibo) {
        if (weibo == null)
            return weibo;

        String content = weibo.getString("content");
        if (!StringUtil.isNullOrEmpty(content)) {
            content = removeNonBmpUnicode(content);
            weibo.put("content", content);

//            content = ChineseCC.toShort(content);
//            content = content.replaceAll(",", "，");

            BanyanTypeUtil.safePut(weibo, "emoji", BanyanTypeUtil.merge(CommonRegexExtractor.extractExpression(content)));
            BanyanTypeUtil.safePut(weibo, "short_link", BanyanTypeUtil.merge(CommonRegexExtractor.extractHyperLink(content)));
            BanyanTypeUtil.safePut(weibo, "topics", BanyanTypeUtil.merge(CommonRegexExtractor.extractHashTag(content)));
            BanyanTypeUtil.safePut(weibo, "mention", BanyanTypeUtil.merge(CommonRegexExtractor.extractMention(content)));

            try {
                // 广告
                AdDetection adDetection = AdDetection.getInstance();
                BanyanTypeUtil.safePut(weibo, "is_ad", adDetection.detection(content));
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                // 分析情感
                MigSentimentExtractor sentimentExtractor = MigSentimentExtractor.getInstance();
                String snt = sentimentExtractor.detection(content);
                BanyanTypeUtil.safePut(weibo, "sentiment", snt);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                //提取关键词
                KeywordsExtractor keywordsExtractor = KeywordsExtractor.getInstance();
                List<String> kwStrings = keywordsExtractor.extract(content);
                if (kwStrings != null && !kwStrings.isEmpty()) {
                    kwStrings = BanyanTypeUtil.topN(kwStrings, KEYWORD_AMOUNT);
//                    Collections.sort(kwStrings);
//                    if (kwStrings.size() > KEYWORD_AMOUNT)
//                        kwStrings = kwStrings.subList(0, KEYWORD_AMOUNT);
                    String concatedKws = Joiner.on(RhinoETLConsts.SEPARATOR).join(kwStrings);
                    BanyanTypeUtil.safePut(weibo, "keywords", concatedKws);
                    BanyanTypeUtil.safePut(weibo, "fingerprint", Md5Util.md5(concatedKws));
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        String source = weibo.getString("source");
        List<String> sourceList = SourceExtractor.extractSources(source);
        if (!CollectionUtil.isEmpty(sourceList))
            BanyanTypeUtil.safePut(weibo, "source", sourceList);

        BanyanTypeUtil.safePut(weibo, "url", WeiboUtils.getWeiboUrl(weibo.getString("uid"), weibo.getString("mid")));

        return weibo;
    }


    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        String content = "秀恩爱[二哈]//@罗晋:你美了[害羞]我哭了[笑cry][哈哈]//@唐嫣:本宝宝受到了惊吓[吃惊][吃惊][吃惊][污][污][污]你们这是要搞事情[抓狂][抓狂][抓狂][抓狂][抓狂][抓狂] http://t.cn/RI7j1Oq";
        content = "";
        Params p = new Params("content", content);
        p = WbContentAnalyzer.getInstance().analyz(p);
        System.out.println(p);
        System.out.println(BanyanTypeUtil.yzStr2List(p.getString("emoji")));

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
