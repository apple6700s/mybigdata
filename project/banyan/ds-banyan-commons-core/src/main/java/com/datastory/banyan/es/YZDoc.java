package com.datastory.banyan.es;

import com.yeezhao.commons.util.serialize.GsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: yaohong
 * Date: 16/7/18
 * Time: 下午7:33
 */
public class YZDoc {
    private static final String FIELD_ID = "id";
    private static final String FIELD_PARENT = "_parent";

    private Map<String, Object> map = new HashMap<String, Object>();

    public YZDoc(String id, Map<String, Object> map) {
        this(map);
        this.map.put(FIELD_ID, id);
    }

    public YZDoc(Map<String, Object> map) {
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (e.getValue() != null) {
                this.put(e.getKey(), e.getValue());
            }
        }
    }

    public YZDoc() {
    }

    public YZDoc(String id) {
        map.put(FIELD_ID, id);
    }

    public Map<String, Object> getSource() {
        return map;
    }

    public String getParent() {
        return (String) map.get(FIELD_PARENT);
    }

    public String getId() {
        return (String) map.get(FIELD_ID);
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public Object get(String key) {
        return map.get(key);
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }

    public Object remove(String key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public String toJson() {
        return GsonSerializer.serialize(map);
    }
}
