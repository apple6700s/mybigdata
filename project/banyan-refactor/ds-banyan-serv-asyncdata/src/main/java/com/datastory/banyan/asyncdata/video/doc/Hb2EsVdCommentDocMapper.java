package com.datastory.banyan.asyncdata.video.doc;

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
public class Hb2EsVdCommentDocMapper extends ParamsDocMapper {
    public Hb2EsVdCommentDocMapper(Params in) {
        super(in);
    }

    public Hb2EsVdCommentDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    protected static final String[] DIRECT_MAP = {
            "cat_id", "item_id", "parent_id",
            "author", "url", "title", "content", "province", "city",
            "site_id", "site_name", "update_date", "publish_date",
            "taskId", "fingerprint",
            "view_cnt", "review_cnt", "like_cnt", "dislike_cnt",
    };

    protected static final Map<String, String> RENAME_MAP = BanyanTypeUtil.strArr2strMap(new String[]{
            "pk", "id",
            "parent_id", "_parent",
    });

    protected static final String[] SHORT = {
            "sentiment", "is_ad", "is_robot"
    };

    @Override
    public Params map() {
        Params out = new Params();
        BanyanTypeUtil.putAllNotNull(out, in, DIRECT_MAP);
        BanyanTypeUtil.putAllNotNull(out, in, RENAME_MAP);

        PublishDateExtractor.extract(out, out.getString("publish_date"));

        List<String> keywords = BanyanTypeUtil.yzStr2List(getString("keywords"));
        BanyanTypeUtil.safePut(out, "keywords", keywords);

        for (String key : SHORT) {
            BanyanTypeUtil.safePut(out, key, getString(key));
        }

        return out;
    }
}
