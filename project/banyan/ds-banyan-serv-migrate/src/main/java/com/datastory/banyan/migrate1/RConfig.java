package com.datastory.banyan.migrate1;

import org.apache.hadoop.conf.Configuration;

/**
 * com.datastory.banyan.migrate1.RConfig
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class RConfig extends Configuration {
    public RConfig() {
        this.addResource("r/core-site.xml");
        this.addResource("r/hdfs-site.xml");
        this.addResource("r/hbase-site.xml");
        this.addResource("r/yarn-site.xml");
        this.addResource("r/mapred-site.xml");

        set("mapreduce.task.timeout", "0");
    }

    private static volatile RConfig _singleton = null;

    public static RConfig getInstance() {
        if (_singleton == null) {
            synchronized (RConfig.class) {
                if (_singleton == null) {
                    _singleton = new RConfig();
                }
            }
        }
        return _singleton;
    }
}
