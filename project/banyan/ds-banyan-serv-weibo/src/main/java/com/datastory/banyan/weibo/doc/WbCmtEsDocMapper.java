package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.WbCmtEsDocMapper
 *
 * @author lhfcws
 * @since 2017/6/2
 */
public class WbCmtEsDocMapper extends ParamsDocMapper {
    public WbCmtEsDocMapper(Params in) {
        super(in);
    }

    public WbCmtEsDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    public static final Map<String, String> FIELDS = BanyanTypeUtil.strArr2strMap(new String[] {
            "cmt_mid", "id",
            "mid", "mid",
            "uid", "cmt_uid",
            "content", "content",
            "keywords", "keywords",
            "fingerprint", "fingerprint",
            "sentiment", "sentiment",
            "update_date", "update_date",
            "publish_date", "publish_date",
            "name", "user_name",
    });

    @Override
    public Params map() {
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, FIELDS);

        short sentiment = BanyanTypeUtil.parseShortForce(out.getString("sentiment"));
        out.put("sentiment", sentiment);

        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                if (out.getString("content") != null) {
                    out.put("content_len", out.getString("content").length());
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

        out.put("_parent", out.getString("mid"));

        return out;
    }
}
