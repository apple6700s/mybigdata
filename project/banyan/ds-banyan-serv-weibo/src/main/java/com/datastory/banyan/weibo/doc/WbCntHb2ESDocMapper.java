package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
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
import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * com.datastory.banyan.weibo.doc.WbCntHb2ESDocMapper
 *
 * @author lhfcws
 * @since 2016/11/9
 */
public class WbCntHb2ESDocMapper extends ParamsDocMapper {
    public WbCntHb2ESDocMapper(Params p) {
        super(p);
    }

    @Override
    public Params map() {
        return simpleTranslate(this.in);
    }

    public static String[] renameMapping = {
            "mid", "id",
            "content", "content",
            "src_content", "src_content",
            "self_content", "self_content",
            "rt_mid", "rt_mid",
            "src_mid", "src_mid",
            "fingerprint", "fingerprint",
            "sentiment", "sentiment",
            "attitudes_cnt", "attitudes_cnt",
            "reposts_cnt", "reposts_cnt",
            "comments_cnt", "comments_cnt",
            "publish_date", "publish_date",
            "uid", "uid",
            "update_date", "update_date",
    };

    public static String[] yzListMapping = {
            "topics",
            "emoji",
            "keywords",
    };

    public static final String[] shortFields = {
            "sentiment", "msg_type"
    };


    public Params simpleTranslate(final Params in) {
        if (in == null || in.size() <= 1)
            return null;

        final Params out = new Params();
        for (int i = 0; i < renameMapping.length; i += 2) {
            if (in.containsKey(renameMapping[i]))
                out.put(renameMapping[i + 1], getString(renameMapping[i]));
        }
        for (String key : yzListMapping) {
            if (in.containsKey(key)) {
                List<String> list = BanyanTypeUtil.yzStr2List(getString(key));
                out.put(key, list);
            }
        }

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (!StringUtil.isNullOrEmpty(getString("source"))) {
                    String source = getString("source");
                    source = source.split(StringUtil.STR_DELIMIT_2ND)[0];
                    out.put("source", source);
                }
                return null;
            }
        });

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (out.getString("content") != null) {
                    out.put("content_len", out.getString("content").length());
                    if (out.getString("self_content") == null) {
                        out.put("self_content", SelfContentExtractor.extract(out.getString("content")));
                    }

                    if (out.getString("self_content") != null)
                        out.put("self_content_len", out.getString("self_content").length());


                    if (out.getString("src_content") != null)
                        out.put("src_content_len", out.getString("src_content").length());
                }
                return null;
            }
        });

        // date
        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                PublishDateExtractor.extract(out, getString("publish_date"));
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
                        List<String> picUrlList = FastJsonSerializer.deserialize(picUrls, CommConsts.TYP_LIST_STR);
                        out.put("pic_urls", picUrlList);
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
                int interactionCnt = 0;
                interactionCnt += BanyanTypeUtil.parseIntForce(in.getString("attitudes_cnt"));
                interactionCnt += BanyanTypeUtil.parseIntForce(in.getString("commentd_cnt"));
                interactionCnt += BanyanTypeUtil.parseIntForce(in.getString("reposts_cnt"));
                out.put("interactions_cnt", interactionCnt + "");
                return null;
            }
        });

        for (String field : shortFields) {
            String v = getString(field);
            if (v != null) {
                out.put(field, BanyanTypeUtil.parseShort(v));
            }
        }

        out.put("_parent", getString("uid"));

        return out;
    }

    @Deprecated
    public Params translate(Params in) {
        if (in == null || in.size() <= 1)
            return null;

        Params out = simpleTranslate(in);

        String srcMid = in.getString("src_mid");
        if (StringUtil.isNullOrEmpty(srcMid))
            return out;

        // try get src_content
        String pk = BanyanTypeUtil.wbcontentPK(srcMid);
        RFieldGetter fieldGetter = new RFieldGetter(Tables.table(Tables.PH_WBUSER_TBL), "content");
        try {
            Map<String, String> res = fieldGetter.get(pk);
            String content = res.get("content");
            if (!StringUtil.isNullOrEmpty(content)) {
                out.put("src_content", content);
                out.put("src_content_len", content.length());
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return out;
    }
}
