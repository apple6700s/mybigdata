package com.datastory.banyan.es;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.math.MathUtils;

import java.util.Map;

/**
 * com.datastory.banyan.es.EsRouting
 * routing for ES 2.3
 *
 * @author lhfcws
 * @since 2017/2/10
 */
public class EsRouting {
    public static final Murmur3HashFunction HASH_FUNC = new Murmur3HashFunction();

    public int routing(String id) {
        return HASH_FUNC.hash(id);
    }

    public int routing(String id, int shards) {
        return MathUtils.mod(routing(id), shards);
    }

    public int routing(Map<String, ? extends Object> mp) {
        String rt = (String) mp.get("_parent");
        if (rt == null) {
            rt = (String) mp.get("id");
        }
        return routing(rt);
    }

    public int routing(Map<String, ? extends Object> mp, int shards) {
        String rt = (String) mp.get("_parent");
        if (rt == null) {
            rt = (String) mp.get("id");
        }
        return routing(rt, shards);
    }
}
