package com.datastory.banyan.asyncdata.ecom.doc;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.asyncdata.ecom.analyz.PriceExtractor;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * com.datastory.banyan.asyncdata.ecom.doc.RhinoVideoPostDocMapper
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class RhinoEcomItemDocMapper extends JSONObjectDocMapper {
    public RhinoEcomItemDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    protected static final Map<String, String> RENAME = BanyanTypeUtil.json2strMap(
            "{" +
                    "    \"unique_id\": \"pk\"," +
                    "    \"site\": \"site_name\"," +
                    "    \"site_id\": \"\"," +
                    "    \"cat_id\": \"\"," +
                    "    \"item_id\": \"\"," +
                    "    \"item_title\": \"title\"," +
                    "    \"item_url\": \"url\"," +
                    "    \"item_image_url\": \"pic_urls\", " +
                    "    \"sell_count_recent\": \"sales_rcnt30_cnt\"," +
                    "    \"sell_count_total\": \"sales_total_cnt\", " +
                    "    \"sell_count_month\": \"sales_month_cnt\"," +
                    "    \"repertory\": \"\"," +
                    "    \"price\": \"\", " +
                    "    \"promo_price\": \"\",   " +
                    "    \"promotion_info\": \"promo_info\"," +
                    "    \"review_count\": \"review_cnt\"," +
                    "    \"good_count\": \"like_cnt\"," +
                    "    \"poor_count\": \"dislike_cnt\", " +
                    "    \"other_data\": \"other_data\"," +
                    "    \"update_date\": \"\"," +
                    "    \"platform_score\": \"\"," +
                    "    \"shop_id\": \"\"," +
                    "    \"shop_name\": \"\"," +
                    "    \"shop_category\": \"\"" +
                    "}");

    @Override
    public Params map() {
        final Params out = new Params();
        out.put("is_parent", "1");
        BanyanTypeUtil.safePut(out, "taskId", jsonObject.getString("taskId"));

        JSONObject srcInfo = jsonObject.getJSONObject("srcInfo");
        if (srcInfo != null) {
            BanyanTypeUtil.safePut(out, "crawl_kw", srcInfo.getString("keyword"));
        }

        this.jsonObject = this.jsonObject.getJSONObject("info");
        BanyanTypeUtil.putAllStrNotNull(out, this, RENAME);

        // price
        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                String price = new PriceExtractor().extractCNY(out.getString("price"));
                String promo_price = new PriceExtractor().extractCNY(out.getString("promo_price"));
                BanyanTypeUtil.safePut(out, "price", price);
                BanyanTypeUtil.safePut(out, "promo_price", promo_price);
                return null;
            }
        });

        return out;
    }
}
