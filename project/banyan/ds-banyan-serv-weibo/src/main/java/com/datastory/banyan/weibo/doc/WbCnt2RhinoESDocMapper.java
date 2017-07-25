package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.config.CommConsts;
import com.yeezhao.commons.util.serialize.GsonSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper
 *
 * @author lhfcws
 * @since 2016/12/22
 */
public class WbCnt2RhinoESDocMapper extends ParamsDocMapper {
    public WbCnt2RhinoESDocMapper(Params in) {
        super(in);
    }

    static final String[] directM = {
            "fingerprint", "content", "update_date", "self_content", "uid", "username"
    };

    static final String[] listM = {
            "topics", "emoji", "keywords", "source"
    };

    static final String[] shortM = {
            "sentiment", "msg_type"
    };

    static final Map<String, String> renameM = BanyanTypeUtil.strArr2strMap(new String[]{
            "uid", "_parent",
            "mid", "id",
            "rt_mid", "pid",
            "src_mid", "retweet_id",
            "attitudes_cnt", "attitudes_count",
            "reposts_cnt", "reposts_count",
            "comments_cnt", "comments_count",
            "src_content", "retweet_content",
    });

    @Override
    public Params map() {
        if (in == null || in.isEmpty())
            return null;
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, directM);
        BanyanTypeUtil.putAllNotNull(out, in, renameM);
        BanyanTypeUtil.putShortNotNull(out, in, shortM);
        BanyanTypeUtil.putYzListNotNull(out, in, listM);

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (in.get("publish_date") != null) {
                    try {
                        Date pDate = DateUtils.parse(in.getString("publish_date"), DateUtils.DFT_TIMEFORMAT);
                        long postTime = pDate.getTime();
                        out.put("post_time", postTime);
                        out.put("post_time_hour", DateUtils.getHourStr(pDate));
                        out.put("post_time_date", DateUtils.getDateStr(pDate));
                    } catch (Exception ignore) {}
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
                String picUrls = in.getString("pic_urls");
                if (BanyanTypeUtil.valid(picUrls)) {
                    try {
                        List<String> picUrlList = GsonSerializer.deserialize(picUrls, CommConsts.TYP_LIST_STR);
                        out.put("pic_urls", picUrlList);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                return null;
            }
        });

        getRetweetContent(out);
        out.put("_parent", getString("uid"));

        return out;
    }

    @Deprecated
    public static Params getRetweetContent(Params out) {
        if (out == null || out.size() <= 1)
            return null;

        String srcMid = out.getString("retweet_id");
        if (StringUtil.isNullOrEmpty(srcMid) || out.get("retweet_content") != null)
            return out;
        String pk = BanyanTypeUtil.wbcontentPK(srcMid);
        RFieldGetter fieldGetter = new RFieldGetter(Tables.table(Tables.PH_WBUSER_TBL), "content");
        try {
            Map<String, String> res = fieldGetter.get(pk);
            if (res != null) {
                String content = res.get("content");
                if (!StringUtil.isNullOrEmpty(content)) {
                    out.put("retweet_content", content);
                    out.put("retweet_content_length", content.length());
                }
            }
        } catch (IOException e) {
            LOG.error(e + " - " + e.getStackTrace()[0]);
        }
        return out;
    }
}
