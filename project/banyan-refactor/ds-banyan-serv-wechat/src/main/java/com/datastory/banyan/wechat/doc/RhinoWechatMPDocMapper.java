package com.datastory.banyan.wechat.doc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Map;

/**
 * com.datastory.banyan.wechat.doc.RhinoWechatMPDocMapper
 *
 * @author lhfcws
 * @since 16/12/12
 */

public class RhinoWechatMPDocMapper extends JSONObjectDocMapper {

    public RhinoWechatMPDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    public static final String[] nestedMapping_ = {
            "desc", "desc",
            "wxid", "wxid",
            "biz", "biz",
    };
    public static final Map<String, String> nestedMapping = BanyanTypeUtil.strArr2strMap(nestedMapping_);

    @Override
    public Params map() {
        if (jsonObject == null || jsonObject.isEmpty())
            return null;

        Params p = new Params();
        p.put("update_date", DateUtils.getCurrentTimeStr());
        try {
            if (jsonObject.containsKey("openid")) {
                String openid = jsonObject.getString("openid");
                p.put("open_id", openid);
            }

            if (jsonObject.containsKey("other_data")) {
                String otherData = jsonObject.getString("other_data");
                JSONObject user = JSON.parseObject(otherData);
                for (Map.Entry<String, String> e : nestedMapping.entrySet()) {
                    if (user.containsKey(e.getKey())) {
                        BanyanTypeUtil.safePut(p, e.getValue(), user.getString(e.getKey()));
                    }
                }
                if (p.get("biz") != null) {
                    p.put("biz", StringEscapeUtils.unescapeJava(p.getString("biz")));
                    p.put("pk", BanyanTypeUtil.sub3PK(p.getString("biz")));
                } else
                    return null;
            } else
                return null;

            if (jsonObject.containsKey("author")) {
                p.put("name", jsonObject.getString("author"));
            }
//            if (jsonObject.containsKey("keyword")) {
//                String keyword = jsonObject.getString("keyword").trim();
//                boolean isOpenID = OpenIdExtractor.isOpenID(keyword);
//                if (isOpenID) {
//                    p.put("open_id", keyword);
//                }
//            }
            return p;
        } catch (Exception e) {
            return null;
        }
    }
}
