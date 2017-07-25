package com.datastory.banyan.asyncdata.ecom.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.asyncdata.ecom.doc.Hb2EsVdPostDocMapper
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class Hb2EsEcomCommentDocMapper extends ParamsDocMapper {
    public Hb2EsEcomCommentDocMapper(Params in) {
        super(in);
    }

    public Hb2EsEcomCommentDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    protected static final String[] DIRECT_MAP = {
            "cat_id", "item_id", "cmt_id", "parent_id",
            "title", "author", "url", "content", "cmt_pic_urls",
            "shop_id", "shop_name", "shop_type",
            "buy_date", "update_date", "publish_date",
            "buy_item", "site_id", "site_name",
            "taskId",
            "fingerprint",
    };

    protected static final Map<String, String> RENAME_MAP = BanyanTypeUtil.strArr2strMap(new String[]{
            "pk", "id",
            "parent_id", "_parent",
    });

    protected static final String[] SHORT = {
            "sentiment", "is_ad", "is_robot", "is_parent",
    };

    protected static final String[] FLOAT = {
            "score"
    };

    @Override
    public Params map() {
        Params out = new Params();
        BanyanTypeUtil.putAllNotNull(out, in, DIRECT_MAP);
        BanyanTypeUtil.putAllNotNull(out, in, RENAME_MAP);

        PublishDateExtractor.extract(out, out.getString("publish_date"));

        List<String> keywords = BanyanTypeUtil.yzStr2List(getString("keywords"));
        BanyanTypeUtil.safePut(out, "keywords", keywords);

        for (String key : FLOAT) {
            BanyanTypeUtil.safePut(out, key, BanyanTypeUtil.parseFloat(getString(key)));
        }

        for (String key : SHORT) {
            BanyanTypeUtil.safePut(out, key, BanyanTypeUtil.parseShort(getString(key)));
        }

        // content length
        BanyanTypeUtil.safePut(out, "content_len", BanyanTypeUtil.len(getString("content")));

        return out;
    }
}
