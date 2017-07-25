package com.dt.mig.sync.base;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by abel.chan on 16/11/20.
 */
public class MigSyncConfiguration extends Configuration {

    private static MigSyncConfiguration config;

    private MigSyncConfiguration() {
        this.addResource("core-site.xml");
        this.addResource("mapred-site.xml");
        this.addResource("rhino/hbase-site.xml");
        this.addResource("hdfs-site.xml");
        this.addResource("yarn-site.xml");
//        this.addResource("conf/mig-sync-conf.xml");
        this.addResource("mig-sync-conf.xml");

//        this.set(ServConsts.PARAM_ES_HOST,
//                "172.18.5.202:9300,172.18.5.205:9300,172.18.5.201:9300,172.18.5.204:9300,172.18.5.203:9300,172.18.5.206:9300");

    }

    public static MigSyncConfiguration getInstance() {
        if (null == config) {
            synchronized (MigSyncConfiguration.class) {
                if (null == config) {
                    config = new MigSyncConfiguration();
                }
            }
        }
        return config;
    }
}
