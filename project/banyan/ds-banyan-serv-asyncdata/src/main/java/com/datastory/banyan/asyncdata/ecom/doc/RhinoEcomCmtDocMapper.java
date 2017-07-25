package com.datastory.banyan.asyncdata.ecom.doc;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Map;

/**
 * com.datastory.banyan.asyncdata.ecom.doc.RhinoVideoPostDocMapper
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class RhinoEcomCmtDocMapper extends JSONObjectDocMapper {
    public RhinoEcomCmtDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    protected static final Map<String, String> RENAME = BanyanTypeUtil.json2strMap(
            "{" +
                    "    \"unique_id\": \"pk\"," +
                    "    \"unique_parent_id\": \"parent_id\"," +
                    "    \"comment_id\": \"cmt_id\"," +
                    "    \"item_id\": \"\"," + //所属的商品的id
                    "    \"site\": \"site_name\"," +
                    "    \"item_title\": \"title\"," +
                    "    \"shop_name\": \"\"," +
                    "    \"content\": \"\"," +
                    "    \"author\": \"\"," +
                    "    \"reference_name\": \"buy_item\"," +
                    "    \"reference_date\": \"buy_date\"," +
                    "    \"score\": \"\"," +
                    "    \"publish_date\": \"\"," +
                    "    \"update_date\": \"\"," +
                    "    \"image_url_list\": \"cmt_pic_urls\"," +
                    "    \"url\": \"\"," +
                    "    \"shop_id\": \"\"," +
                    "    \"shop_category\": \"shop_type\"" +
                    "}"
    );

    @Override
    public Params map() {
        Params out = new Params();
        out.put("is_parent", "0");
        BanyanTypeUtil.safePut(out, "taskId", jsonObject.getString("taskId"));

        this.jsonObject = this.jsonObject.getJSONObject("info");
        BanyanTypeUtil.putAllStrNotNull(out, this, RENAME);

        return out;
    }
}
