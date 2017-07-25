package com.datastory.banyan.wechat.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;

import javax.annotation.Nullable;

/**
 * com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxCntHb2ESDocMapper extends ParamsDocMapper {
    public WxCntHb2ESDocMapper(Params in) {
        super(in);
    }

    public static final String[] mapping = {
            "content", "title", "author", "update_date",
            "like_cnt", "view_cnt", "url",
            "is_original", "brief", "thumbnail", "src_url",

            "wxid", "biz", "article_id", "sn", "mid", "idx",

            "fingerprint",
    };

    public static final String AUTHOR_ID_FIELD = "biz";

    @Override
    public Params map() {
        if (in == null || in.size() <= 1)
            return null;

        final Params esDoc = new Params();

        esDoc.put("id", getString("pk"));
        esDoc.put("author_id", getString(AUTHOR_ID_FIELD));

        for (String field : mapping)
            esDoc.put(field, getString(field));

        PublishDateExtractor.extract(esDoc, getString("publish_date"));

        esDoc.put("content_len", BanyanTypeUtil.len(getString("content")));
        esDoc.put("title_len", BanyanTypeUtil.len(getString("title")));

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                String keywords = getString("keywords");
                if (keywords != null) {
                    BanyanTypeUtil.safePut(esDoc, "keywords", BanyanTypeUtil.yzStr2List(keywords));
                }
                return null;
            }
        });

        BanyanTypeUtil.safePut(esDoc, "sentiment", getShort("sentiment"));
        BanyanTypeUtil.safePut(esDoc, "is_ad", getShort("is_ad"));

        esDoc.put("_parent", getString(AUTHOR_ID_FIELD));
        return esDoc;
    }
}
