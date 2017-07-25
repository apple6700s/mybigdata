package com.dt.mig.sync.hbase.doc;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.extract.SelfContentExtractor;
import com.dt.mig.sync.hbase.RFieldGetter;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.DateUtils;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.WbCnt2MigESDocMapper
 *
 * @author lhfcws
 * @since 2016/12/22
 */
public class WbCnt2MigESDocMapper extends ParamsDocMapper {
    public WbCnt2MigESDocMapper(Params in) {
        super(in);
    }

    static final String[] directM = {"fingerprint", "content", "update_date", "self_content", "uid", "username", "is_real"};

    static final String[] listM = {"topics", "emoji", "keywords", "source"};

    static final String[] shortM = {"sentiment", "msg_type"};

    static final Map<String, String> renameM = BanyanTypeUtil.strArr2strMap(new String[]{
//            "uid", "_parent",
            "mid", "id", "rt_mid", "pid", "src_mid", "retweet_id", "attitudes_cnt", "attitudes_count", "reposts_cnt", "reposts_count", "comments_cnt", "comments_count", "src_content", "retweet_content",});

    @Override
    public Params map() {
        if (in == null || in.isEmpty()) return null;
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, directM);
        BanyanTypeUtil.putAllNotNull(out, in, renameM);
        BanyanTypeUtil.putShortNotNull(out, in, shortM);
        BanyanTypeUtil.putYzListNotNull(out, in, listM);

        if (in.containsKey("comments")) {
            out.put("comments", in.get("comments"));
        }
        if (in.containsKey("reposts")) {
            out.put("reposts", in.get("reposts"));
        }
        if (in.containsKey("comments")) {
            out.put("attitudes", in.get("attitudes"));
        }


        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                out.put("update_date", DateUtils.getCurrentTimeStr());
                if (in.get("publish_date") != null) {
                    try {
                        Date pDate = DateUtils.parse(in.getString("publish_date"), DateUtils.DFT_TIMEFORMAT);
                        long postTime = pDate.getTime();
                        out.put("post_time", postTime);
                        out.put("post_time_hour", DateUtils.getHourStr(pDate));
                        out.put("post_time_date", DateUtils.getDateStr(pDate));
                    } catch (Exception ignore) {
                    }
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (out.getString("content") != null) {
                    out.put("content_length", out.getString("content").length());
                    if (out.getString("self_content") == null) {
                        out.put("self_content", SelfContentExtractor.extract(out.getString("content")));
                    }

                    if (out.getString("self_content") != null)
                        out.put("self_content_length", out.getString("self_content").length());


                    if (out.getString("retweet_content") != null)
                        out.put("retweet_content_length", out.getString("retweet_content").length());
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                out.put("interactions_count", BanyanTypeUtil.parseIntForce(in.getString("reposts_cnt")) + BanyanTypeUtil.parseIntForce(in.getString("comments_cnt")) + BanyanTypeUtil.parseIntForce(in.getString("attitudes_cnt")));

                out.put("repost_comment_count", BanyanTypeUtil.parseIntForce(in.getString("reposts_cnt")) + BanyanTypeUtil.parseIntForce(in.getString("comments_cnt")));

                return null;
            }
        });

        getRetweetContent(out);

        return out;
    }

    @Deprecated
    public Params getRetweetContent(Params out) {
        if (out == null || out.size() <= 1) return null;

        String srcMid = out.getString("retweet_id");
        if (StringUtil.isNullOrEmpty(srcMid) || out.get("retweet_content") != null) return out;
        String pk = BanyanTypeUtil.wbcontentPK(srcMid);
        RFieldGetter fieldGetter = new RFieldGetter("DS_BANYAN_WEIBO_CONTENT_V1", "content");
        try {
            Params res = fieldGetter.get(pk);
            String content = res.getString("content");
            if (!StringUtil.isNullOrEmpty(content)) {
                out.put("retweet_content", content);
                out.put("retweet_content_length", content.length());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out;
    }

    public YZDoc yzDoc() {
        Params out = map();
        return new YZDoc(out);
    }
}
