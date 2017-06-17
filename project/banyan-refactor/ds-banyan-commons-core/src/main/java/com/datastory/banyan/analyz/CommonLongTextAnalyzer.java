package com.datastory.banyan.analyz;

import com.datastory.banyan.monitor.stat.TimeStatMap;
import com.datatub.scavenger.extractor.AdDetection;
import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.datatub.scavenger.extractor.MigNewsSntExtractor;
import com.yeezhao.commons.util.Entity.Document;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.encypt.Md5Util;
import com.yeezhao.hornbill.algo.astrotufing.classifier.UselessPostClassifier;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.List;

/**
 * com.datastory.banyan.analyz.CommonLongTextAnalyzer
 *
 * @author lhfcws
 * @since 16/12/13
 */

public class CommonLongTextAnalyzer implements Serializable {
    protected static Logger LOG = Logger.getLogger(CommonLongTextAnalyzer.class);
    protected static MigNewsSntExtractor migNewsSntExtractor = null;
    protected static KeywordsExtractor keywordsExtractor = null;
    protected static AdDetection adDetection = null;
    protected static UselessPostClassifier uselessPostClassifier = null;
    protected boolean enableLog = false;

    public CommonLongTextAnalyzer() {
        init();
    }

    public CommonLongTextAnalyzer(boolean enableLog) {
        this();
        this.enableLog = enableLog;
    }

    private static volatile CommonLongTextAnalyzer _singleton = null;

    public static CommonLongTextAnalyzer getInstance() {
        if (_singleton == null) {
            synchronized (CommonLongTextAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new CommonLongTextAnalyzer();
                }
            }
        }
        return _singleton;
    }

    private void init() {
        migNewsSntExtractor = MigNewsSntExtractor.getInstance();
        keywordsExtractor = KeywordsExtractor.getInstance();
        try {
            adDetection = AdDetection.getInstance();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
        }
        uselessPostClassifier = UselessPostClassifier.getInstance();
    }

    public boolean isMainPost(Params p) {
        String isMainPost = p.getString("is_main_post");

        if (isMainPost != null)
            return "1".equals(isMainPost);
        else {
            return !p.containsKey("parent_post_id") && !p.containsKey("parent_id");
        }
    }

    public static String removeNonBmpUnicode(String str) {
        if (str == null) {
            return null;
        }
        str = str.replaceAll("[^\\u0000-\\uFFFF]", "");
        return str;
    }

    public Params _beforeAnalyz(Params p) {
        return p;
    }

    public Params _afterAnalyz(Params p) {
        return p;
    }

    public Params analyz(Params p) {
        if (p == null || p.isEmpty())
            return p;

        TimeStatMap statMap = new TimeStatMap();

//        statMap.begin("simplify");
        String title = p.getString("title");
//        if (title != null) {
//            title = ChineseCC.toShort(title);
//            title = title.replaceAll(",", "，");
//            p.put("title", title);
//        }
//        if (content != null) {
//            content = StringEscapeUtils.escapeJava(content);
//            content = ChineseCC.toShort(content);
//            content = content.replaceAll(",", "，");
//            p.put("content", content);
//        }
//        statMap.end("simplify");

        statMap.begin("beforeAnalyz");
        p = _beforeAnalyz(p);
        statMap.end("beforeAnalyz");

        String content = p.getString("content");
        if (!StringUtil.isNullOrEmpty(content)) {
            content = removeNonBmpUnicode(content);

            Document document = new Document(Document.Field.Text, title + content);
            document.putField(Document.Field.Threshold, "3");   // supported by MouHao

            // keywords
            statMap.begin("keyword");
            if (keywordsExtractor != null) {
                List<String> keywords = keywordsExtractor.extract(title + content);
                int len = Math.min(5, keywords.size());
                StringBuilder sb = new StringBuilder();
                int i = 0;
                for (String kw : keywords) {
                    if (i >= len) break;
                    sb.append(kw).append("|");
                    i++;
                }
                if (i > 0)
                    sb.setLength(sb.length() - 1);
                String keywordStr = sb.toString();
                String fingerprint = Md5Util.md5(keywordStr);
                p.put("fingerprint", fingerprint);
                p.put("keywords", keywordStr);
            }
            statMap.end("keyword");

            // addetection
            statMap.begin("ad");
            if (adDetection != null) {
                try {
                    String isAD = adDetection.detection(content);
                    p.put("is_ad", isAD);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
            statMap.end("ad");

            // useless
            statMap.begin("useless");
            if (uselessPostClassifier != null) {
                String isRobot = uselessPostClassifier.classify(content);
                p.put("is_robot", isRobot);
            }
            statMap.end("useless");

            // sentiment
            statMap.begin("sentiment");
            if (migNewsSntExtractor != null) {
                String sentiment = migNewsSntExtractor.classify(document);
                p.put("sentiment", sentiment);
            }
            statMap.end("sentiment");

            p.put("content", content);
        } else {
            p.put("is_robot", isMainPost(p) ? "1" : "0");
        }

        statMap.begin("afterAnalyz");
        p = _afterAnalyz(p);
        statMap.end("afterAnalyz");

        if (enableLog) {
            LOG.info(statMap.toString());
        }

        return p;
    }
}
