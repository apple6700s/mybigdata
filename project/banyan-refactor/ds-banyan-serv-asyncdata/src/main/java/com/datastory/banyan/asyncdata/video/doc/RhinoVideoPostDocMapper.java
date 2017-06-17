package com.datastory.banyan.asyncdata.video.doc;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
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
        BanyanTypeUtil.putAllStrNotNull(out, this, RENAME);
        out = CommonLongTextAnalyzer.getInstance().analyz(out);
        return out;
    }
}
