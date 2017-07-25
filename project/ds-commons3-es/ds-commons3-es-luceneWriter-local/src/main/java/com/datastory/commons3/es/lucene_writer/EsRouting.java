package com.datastory.commons3.es.lucene_writer;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.math.MathUtils;

import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.EsRouting
 * routing for ES 2.3
 *
 * @author lhfcws
 * @since 2017/2/10
 */
public class EsRouting {
    public static final Murmur3HashFunction HASH_FUNC = new Murmur3HashFunction();

    private static int routing(String id) {
        return HASH_FUNC.hash(id);
    }

    public static int routing(String id, int shards) {
        return MathUtils.mod(routing(id), shards);
    }

    public static int routing(Map<String, ? extends Object> mp, int shards) {
        String rt = (String) mp.get("_parent");
        if (rt == null) {
            rt = (String) mp.get("id");
        }
        return routing(rt, shards);
    }
}
