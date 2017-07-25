package com.datastory.banyan.weibo.doc;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringUtils;

/**
 * com.datastory.banyan.weibo.doc.RhinoTrendDocMapper
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class RhinoTrendDocMapper extends JSONObjectDocMapper {
    public RhinoTrendDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    @Override
    public Params map() {
        if (jsonObject == null || jsonObject.isEmpty())
            return null;

        Params p = new Params();
        p.put("parent_id", getString("uid"));
        p.put("id", getString("mid"));
        p.put("update_date", getString("update_date"));
        p.put("source", RhinoETLConsts.SRC_WB);
        p.put("type", "rca");

        String data = StringUtils.join(new String[] {
                getString("reposts"), getString("comments"), getString("attitudes")
        }, "|");
        p.put("data", data);

        String pk = StringUtils.join(new String[] {
                p.getString("source"), p.getString("id"), p.getString("type")
        }, "|");
        p.put("pk", pk);

        return p;
    }
}
