package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.WbCmtWeiboEsDocMapper
 *
 * @author lhfcws
 * @since 2017/6/2
 */
public class WbCmtWeiboEsDocMapper extends ParamsDocMapper {
    public WbCmtWeiboEsDocMapper(Params in) {
        super(in);
    }

    public WbCmtWeiboEsDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    public static final Map<String, String> FIELDS = BanyanTypeUtil.strArr2strMap(new String[] {
            "mid", "id",
            "uid", "uid",
            "content", "content",
            "src_content", "src_content",
            "self_content", "self_content",
            "sentiment", "sentiment",
            "msg_type", "msg_type",
            "update_date", "update_date",
            "publish_date", "publish_date",
            "rt_mid", "rt_mid",
            "src_mid", "src_mid",
            "src_uid", "src_uid",
    });

    @Override
    public Params map() {
        final Params out = new Params();

        BanyanTypeUtil.putAllNotNull(out, in, FIELDS);

        short sentiment = BanyanTypeUtil.parseShortForce(out.getString("sentiment"));
        out.put("sentiment", sentiment);
        short msgType = BanyanTypeUtil.parseShortForce(out.getString("msg_type"));
        out.put("msg_type", msgType);

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

        return out;
    }
}
