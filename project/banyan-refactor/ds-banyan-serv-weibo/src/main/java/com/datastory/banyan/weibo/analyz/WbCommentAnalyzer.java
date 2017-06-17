package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.datatub.scavenger.extractor.MigSentimentExtractor;
import com.google.common.base.Joiner;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.encypt.Md5Util;

import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.WbCommentAnalyzer
 *
 * @author lhfcws
 * @since 16/12/15
 */

public class WbCommentAnalyzer {
    protected static final int KEYWORD_AMOUNT = 3;
    private static volatile WbCommentAnalyzer _singleton = null;

    public static WbCommentAnalyzer getInstance() {
        if (_singleton == null)
            synchronized (WbCommentAnalyzer.class) {
                if (_singleton == null) {
                    _singleton = new WbCommentAnalyzer();
                }
            }
        return _singleton;
    }

    protected MigSentimentExtractor sntExtractor = null;
    protected KeywordsExtractor keywordsExtractor = null;

    public WbCommentAnalyzer() {
        try {
            sntExtractor = MigSentimentExtractor.getInstance();
            keywordsExtractor = KeywordsExtractor.getInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Params migrateAnalyz(Params cmt) {
        if (cmt == null || cmt.isEmpty())
            return cmt;

        String content = cmt.getString("content");
        if (!StringUtil.isNullOrEmpty(content)) {
            content = content.trim();

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
                    BanyanTypeUtil.safePut(cmt, "keywords", concatedKws);
                    BanyanTypeUtil.safePut(cmt, "fingerprint", Md5Util.md5(concatedKws));
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cmt;
    }

    public Params analyz(Params cmt) {
        if (cmt == null || cmt.isEmpty())
            return cmt;

        if (!cmt.containsKey("cmt_mid") && cmt.containsKey("pk")) {
            String pk = cmt.getString("pk");
            cmt.put("cmt_mid", pk.substring(3));
        }

        String content = cmt.getString("content");
        if (!StringUtil.isNullOrEmpty(content)) {
            content = content.trim();

            try {
                String snt = null;
                snt = sntExtractor.detection(content);
                BanyanTypeUtil.safePut(cmt, "sentiment", snt);
            } catch (Exception e) {
                e.printStackTrace();
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
                    BanyanTypeUtil.safePut(cmt, "keywords", concatedKws);
                    BanyanTypeUtil.safePut(cmt, "fingerprint", Md5Util.md5(concatedKws));
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cmt;
    }
}
