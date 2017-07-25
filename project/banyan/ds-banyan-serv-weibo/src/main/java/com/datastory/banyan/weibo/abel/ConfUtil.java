package com.datastory.banyan.weibo.abel;

import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * com.datatub.rhino.utils.ConfUtil
 *
 * @author lhfcws
 * @since 2016/11/2
 */
public class ConfUtil {
    public static Configuration addConf(Configuration conf, Configuration add) {
        Iterator<Map.Entry<String, String>> iter = add.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> e = iter.next();
            conf.set(e.getKey(), e.getValue());
        }
        return conf;
    }

    public static Configuration addConf(Configuration conf, String resource) {
        Configuration add = empty();
        add.addResource(resource);
        return addConf(conf, add);
    }

    public static Configuration empty() {
        Configuration conf = new Configuration();
        conf.clear();
        return conf;
    }

    public static Configuration resourceConf(String resource) {
        Configuration conf = empty();
        conf.addResource(resource);
        return conf;
    }
}
