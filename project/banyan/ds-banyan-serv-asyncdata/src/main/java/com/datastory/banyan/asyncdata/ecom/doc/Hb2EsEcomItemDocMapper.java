package com.datastory.banyan.asyncdata.ecom.doc;

import com.datastory.banyan.analyz.PublishDateExtractor;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Map;

/**
 * com.datastory.banyan.asyncdata.ecom.doc.Hb2EsVdPostDocMapper
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class Hb2EsEcomItemDocMapper extends ParamsDocMapper {
    public Hb2EsEcomItemDocMapper(Params in) {
        super(in);
    }

    public Hb2EsEcomItemDocMapper(Map<String, ? extends Object> mp) {
        super(mp);
    }

    protected static final String[] DIRECT_MAP = {
            "cat_id", "item_id", "title", "name",
            "sales_total_cnt", "sales_month_cnt", "sale_rcnt30_cnt",
            "shop_id", "shop_name", "shop_type",
            "platform_score", "repertory", "promo_info", "currency",
            "review_cnt", "like_cnt", "dislike_cnt",
            "url", "pic_urls", "site_id", "site_name",
            "publish_date", "update_date",
            "taskId", "crawl_kw",
    };

    protected static final Map<String, String> RENAME_MAP = BanyanTypeUtil.strArr2strMap(new String[]{
            "pk", "id"
    });

    protected static final String[] FLOAT = {
            "price", "promo_price", "platform_score"
    };

    protected static final String[] SHORT = {
            "is_parent"
    };

    @Override
    public Params map() {
        Params out = new Params();
        BanyanTypeUtil.putAllNotNull(out, in, DIRECT_MAP);
        BanyanTypeUtil.putAllNotNull(out, in, RENAME_MAP);

        PublishDateExtractor.extract(out, out.getString("publish_date"));

        for (String key : FLOAT) {
            BanyanTypeUtil.safePut(out, key, BanyanTypeUtil.parseFloat(getString(key)));
        }

        for (String key : SHORT) {
            BanyanTypeUtil.safePut(out, key, BanyanTypeUtil.parseShort(getString(key)));
        }

        return out;
    }
}
