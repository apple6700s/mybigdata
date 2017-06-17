package com.datastory.banyan.wechat.doc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Map;

/**
 * com.datastory.banyan.wechat.doc.RhinoWechatContentDocMapper
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class RhinoWechatContentDocMapper extends JSONObjectDocMapper {
    public RhinoWechatContentDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    public static final String[] mapping_ = {
            "item_id", "pk",
            "like_count", "like_cnt",
            "view_count", "view_cnt",
            "url", "url",
            "src_url", "src_url",
            "title", "title",
            "content", "content",
            "author", "author",
            "update_date", "update_date",
            "is_original", "is_original",
            "publish_date", "publish_date",
            "brief", "brief",
            "thumbnail", "thumbnail",
    };
    public static final Map<String, String> mapping = BanyanTypeUtil.strArr2strMap(mapping_);

    public static final String[] nestedMapping_ = {
            "wxAuthor", "wx_author",
            "wxid", "wxid",
            "biz", "biz",
            "sn", "sn",
            "mid", "mid",
            "idx", "idx",
    };
    public static final Map<String, String> nestedMapping = BanyanTypeUtil.strArr2strMap(nestedMapping_);

    @Override
    public Params map() {
        if (jsonObject == null || jsonObject.isEmpty())
            return null;

        Params p = new Params();

        for (String oldField : mapping.keySet())
            if (jsonObject.containsKey(oldField)) {
                Object v = jsonObject.get(oldField);
                if (v != null)
                    p.put(mapping.get(oldField), String.valueOf(v));
            }

        try {
            String otherDataStr = jsonObject.getString("other_data");
            if (otherDataStr != null && otherDataStr.trim().length() > 0) {
                JSONObject otherData = JSON.parseObject(otherDataStr);
                for (String oldField : nestedMapping.keySet())
                    if (otherData.containsKey(oldField)) {
                        Object v = otherData.get(oldField);
                        if (v != null)
                            p.put(nestedMapping.get(oldField), String.valueOf(v));
                    }

                if (p.get("biz") != null)
                    p.put("biz", StringEscapeUtils.unescapeJava(p.getString("biz")));

            }
        } catch (Exception e) {
            ErrorUtil.simpleError(e);
        }

        return p;
    }
}
