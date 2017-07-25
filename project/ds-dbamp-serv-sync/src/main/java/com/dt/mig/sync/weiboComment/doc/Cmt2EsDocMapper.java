package com.dt.mig.sync.weiboComment.doc;

import com.datatub.scavenger.extractor.KeywordsExtractor;
import com.dt.mig.sync.hbase.doc.ParamsDocMapper;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.DateUtils;
import com.google.common.base.Joiner;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.encypt.Md5Util;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.weiboComment.doc.Cmt2EsDocMapper
 *
 * @author lhfcws
 * @since 2017/4/5
 */
public class Cmt2EsDocMapper extends ParamsDocMapper {
    public static final int KEYWORD_AMOUNT = 3;
    protected Params cnt;
    protected Params user;

    public Cmt2EsDocMapper(Params in) {
        super(in);
    }

    public Cmt2EsDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    public Cmt2EsDocMapper setParams(Params cnt, Params user) {
        this.cnt = cnt;
        this.user = user;
        return this;
    }

    protected static final Map<String, String> cntMapping = BanyanTypeUtil.strArr2strMap(new String[]{"src_mid", "retweet_id", "msg_type", "weibo_msg_type",});

    protected static final Map<String, String> userMapping = BanyanTypeUtil.strArr2strMap(new String[]{"verified_type", "verified_type", "user_type", "user_type", "meta_group", "meta_group", "birthyear", "birth_year", "city", "city", "province", "province", "name", "user_name", "gender", "gender",});

    protected static final Map<String, String> cmtMapping = BanyanTypeUtil.strArr2strMap(new String[]{"cmt_id", "id", "mid", "mid", "uid", "uid", "publish_date", "comment_date", "fingerprint", "fingerprint", "sentiment", "sentiment", "content", "content", "mig_mention", "mig_mention"});

    @Override
    public Params map() {
        Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, cnt, cntMapping);
        if (user != null) {
            BanyanTypeUtil.putAllNotNull(out, user, userMapping);
        }
        BanyanTypeUtil.putAllNotNull(out, in, cmtMapping);


//        protected static final Map<String, String> userMapping = BanyanTypeUtil.strArr2strMap(new String[]{
//                "verified_type", "verified_type",
//                "user_type", "user_type",
//                "meta_group", "meta_group",
//                "birthyear", "birth_year",
//                "city", "city",
//                "province", "province",
//                "name", "user_name",
//                "gender", "gender",
//        });

        if (isExistField(out, "user_type")) {
            BanyanTypeUtil.safePut(out, "user_type", BanyanTypeUtil.parseShort(out.getString("user_type")));
        }

        if (isExistField(out, "verified_type")) {
            BanyanTypeUtil.safePut(out, "verified_type", BanyanTypeUtil.parseShort(out.getString("verified_type")));
        }

        if (isExistField(out, "weibo_msg_type")) {
            BanyanTypeUtil.safePut(out, "weibo_msg_type", BanyanTypeUtil.parseShort(out.getString("weibo_msg_type")));
        }


        if (isExistField(out, "sentiment")) {
            BanyanTypeUtil.safePut(out, "sentiment", BanyanTypeUtil.parseShort(out.getString("sentiment")));
        }


        if (isExistField(out, "gender")) {
            BanyanTypeUtil.safePut(out, "gender", BanyanTypeUtil.parseShort(out.getString("gender")));
        }

        // comment_hour / comment_time
        try {
            String dateStr = out.getString("comment_date");
            Date date = DateUtils.parse(dateStr, DateUtils.DFT_TIMEFORMAT);
            String hourStr = DateUtils.getHourStr(date);
            long ts = date.getTime();

            out.put("comment_hour", hourStr);
            out.put("comment_time", ts);
        } catch (Exception e) {
            e.printStackTrace();
        }


        // content_length
        out.put("content_length", BanyanTypeUtil.len(out.getString("content")));

        // fixed for abel : mig mention 移至wbCommentSync里面做分析


        // keywords
        if (out.containsKey("keywords")) {
            BanyanTypeUtil.safePut(out, "keywords", BanyanTypeUtil.yzStr2List(getString("keywords")));
        } else {
            try {
                //提取关键词
                String content = out.getString("content");
                KeywordsExtractor keywordsExtractor = KeywordsExtractor.getInstance();
                List<String> kwStrings = keywordsExtractor.extract(content);
                if (kwStrings != null && !kwStrings.isEmpty()) {
                    kwStrings = BanyanTypeUtil.topN(kwStrings, KEYWORD_AMOUNT);
                    BanyanTypeUtil.safePut(out, "keywords", kwStrings);
                    String concatedKws = Joiner.on("|").join(kwStrings);
                    BanyanTypeUtil.safePut(out, "fingerprint", Md5Util.md5(concatedKws));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return out;
    }

    public boolean isExistField(Params params, String field) {
        if (params != null && params.containsKey(field)) {
            return true;
        }
        return false;
    }
}
