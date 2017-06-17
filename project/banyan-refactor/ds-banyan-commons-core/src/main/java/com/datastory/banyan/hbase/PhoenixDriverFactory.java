package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.hadoop.conf.Configuration;


/**
 * com.datastory.banyan.hbase.PhoenixDriverFactory
 * PhoenixDriver工厂类，调用Driver的对象应尽量不关心这些构造。
 * @author lhfcws
 * @since 2016/10/17
 */
public class PhoenixDriverFactory {
    public static PhoenixDriver getDriver() {
        Configuration conf = RhinoETLConfig.getInstance();
        String zkQuorum = conf.get("hbase.zookeeper.quorum");
        int zkPort = conf.getInt("hbase.zookeeper.property.clientPort", 2181);
        return new PhoenixDriver(zkQuorum, zkPort, "/hbase-unsecure");
    }

    public static String getDriverZKConn() {
        Configuration conf = RhinoETLConfig.getInstance();
        String zkQuorum = conf.get("hbase.zookeeper.quorum");
        int zkPort = conf.getInt("hbase.zookeeper.property.clientPort", 2181);
        return zkQuorum + ":" + zkPort;
    }
}
