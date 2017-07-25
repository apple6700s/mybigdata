package com.datastory.banyan.asyncdata.video.doc;

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
public class RhinoVideoPostDocMapper extends JSONObjectDocMapper {
    public RhinoVideoPostDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    protected static final Map<String, String> RENAME = BanyanTypeUtil.json2strMap(
            "{" +
                    "    \"unique_id\": \"pk\"," +
                    "    \"item_id\": \"\"," +
                    "    \"cat_id\": \"\"," +
                    "    \"site_id\": \"\"," +
                    "    \"site\": \"site_name\"," +
                    "    \"title\": \"\"," +
                    "    \"url\": \"\"," +
                    "    \"content\": \"\"," +
                    "    \"author\": \"\"," +
                    "    \"view_count\": \"view_cnt\"," +
                    "    \"like_count\": \"like_cnt\"," +
                    "    \"dislike_count\": \"dislike_cnt\"," +
                    "    \"review_count\": \"review_cnt\"," +
                    "    \"publish_date\": \"\"," +
                    "    \"update_date\": \"\"," +
                    "    \"type\": \"channel\"," +
                    "    \"album_title\":\"album\"" +
                    "}"
    );

    @Override
    public Params map() {
        Params out = new Params();
        out.put("is_parent", "1");
        BanyanTypeUtil.safePut(out, "taskId", jsonObject.getString("taskId"));

        BanyanTypeUtil.safePut(out, "crawl_kw", jsonObject.getJSONObject("srcInfo").getString("keyword"));

        this.jsonObject = this.jsonObject.getJSONObject("info");

        BanyanTypeUtil.putAllStrNotNull(out, this, RENAME);
        return out;
    }
}
