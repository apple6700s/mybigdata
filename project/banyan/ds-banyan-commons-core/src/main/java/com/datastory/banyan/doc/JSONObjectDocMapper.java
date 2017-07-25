package com.datastory.banyan.doc;

import com.alibaba.fastjson.JSONObject;

/**
 * com.datastory.banyan.doc.JSONObjectDocMapper
 *
 * @author lhfcws
 * @since 16/11/24
 */

public abstract class JSONObjectDocMapper extends DocMapper {
    protected JSONObject jsonObject;

    public JSONObjectDocMapper(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    @Override
    public String getString(String key) {
        if (hasKey(key))
            return jsonObject.getString(key);
        else
            return null;
    }

    @Override
    public Integer getInt(String key) {
        if (hasKey(key))
            return jsonObject.getInteger(key);
        else
            return null;
    }

    public boolean hasKey(String key) {
        return jsonObject.containsKey(key);
    }
}
