package com.datastory.banyan.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.Serializable;


/**
 * @author sezina
 * @since 7/14/16
 */
public class RhinoETLConfig extends Configuration implements Serializable {

    private static final Logger LOG = Logger.getLogger(RhinoETLConfig.class);

    private volatile static RhinoETLConfig config;

    private RhinoETLConfig() {
        LOG.info("initing BanyanETLConfig.");
        addResource("banyan-etl-config.xml");
        addResource("email-config.xml");

        addResource("core-site.xml");
        addResource("hbase-site.xml");
        addResource("hdfs-site.xml");
        addResource("mapred-site.xml");
        addResource("yarn-site.xml");
        LOG.info("done init BanyanETLConfig. hbase.zookeeper.quorum=" + this.get("hbase.zookeeper.quorum"));
    }

    public static RhinoETLConfig getInstance() {
        if (null == config) {
            synchronized (RhinoETLConfig.class) {
                if (null == config) {
                    config = new RhinoETLConfig();
                }
            }
        }
        config.set("hbase.client.ipc.pool.type", "RoundRobinPool");
        config.set("hbase.client.ipc.pool.size", "10");
        config.set("hbase.rpc.timeout", "900000");
        config.set("hbase.client.scanner.timeout.period", "900000");
        config.set("hbase.rpc.shortoperation.timeout", "300000");
        return config;
    }

    public static Configuration create() {
        return new Configuration(getInstance());
    }

    public String getKafkaZkQuorum() {
        String zooKeeper = this.get(RhinoETLConsts.KAFKA_ZOOKEEPER_LIST);
        String port = this.get(RhinoETLConsts.KAFKA_ZOOKEEPER_PORT);
        if (zooKeeper != null && port != null) {

            StringBuilder sb = new StringBuilder();
            String[] arr = zooKeeper.split(",");
            for (String s : arr) {
                sb.append(s).append(":").append(port).append(",");
            }
            String tmp = sb.toString();
            return tmp.substring(0, tmp.length() - 1);
        }
        return null;
    }

    public String getHBaseZkQuorum() {
        String zooKeeper = this.get(RhinoETLConsts.ZOOKEEPER_LIST);
        String port = this.get(RhinoETLConsts.ZOOKEEPER_PORT);
        if (zooKeeper != null && port != null) {

            StringBuilder sb = new StringBuilder();
            String[] arr = zooKeeper.split(",");
            for (String s : arr) {
                sb.append(s).append(":").append(port).append(",");
            }
            String tmp = sb.toString();
            return tmp.substring(0, tmp.length() - 1);
        }
        return null;
    }
}
