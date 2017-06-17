package com.datastory.banyan.weibo.doc;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.Rhino2CmtParamsDocMapper
 *
 * @author lhfcws
 * @since 16/11/25
 */

public class Rhino2CmtParamsDocMapper extends JSONObjectDocMapper {
    public Rhino2CmtParamsDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    private static final String[] renameM_ = {
            "uid", "uid",
            "mid", "cmt_mid",
            "parent_id", "mid",
            "publish_date", "publish_date",
            "update_date", "update_date",
    };
    private static final Map<String, String> renameM = BanyanTypeUtil.strArr2strMap(renameM_);

    @Override
    public Params map() {
        Params p = new Params();

        BanyanTypeUtil.putAllNotNull(p, jsonObject, renameM);

        if (hasKey("json")) {
            JSONObject o = JSONObject.parseObject(getString("json"));

            if (o.containsKey("text"))
                p.put("content", o.getString("text"));
            if (o.containsKey("user")) {
                JSONObject u = JSONObject.parseObject(o.getString("user"));
                if (u.containsKey("id"))
                    p.put("uid", u.getString("id"));
                if (u.containsKey("name"))
                    p.put("name", u.getString("name"));
            }
        }

        String pk = BanyanTypeUtil.sub3PK(p.getString("cmt_mid"));
        p.put("pk", pk);

        return p;
    }
}
