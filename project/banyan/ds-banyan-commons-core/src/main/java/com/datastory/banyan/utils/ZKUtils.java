package com.datastory.banyan.utils;


import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.zookeeper.*;

import java.io.IOException;


/**
 * com.datatub.rhino.utils.ZKUtils
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public class ZKUtils {
    static final String zkConn = RhinoETLConfig.getInstance().getKafkaZkQuorum();

    public static ZooKeeper getZK() throws IOException {
        return new ZooKeeper(zkConn, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
            }
        });
    }

    public static void createGroupOffsetIfNexist(String group, String topic, int partition) {
        String groupPath = "/consumers/" + group;
        String path = String.format("/consumers/%s/offsets/%s/%d", group, topic, partition);
        try {
            ZooKeeper zk = null;
            try {
                zk = getZK();
                if (zk.exists(groupPath, false) == null) {
                    zk.create(groupPath, "null".getBytes(), null, CreateMode.PERSISTENT);
                    zk.create(groupPath + "/offsets", "null".getBytes(), null, CreateMode.PERSISTENT);
                    zk.create(groupPath + "/offsets/" + topic, "null".getBytes(), null, CreateMode.PERSISTENT);
                }
                if (zk.exists(path, false) == null)
                    zk.create(path, "-1".getBytes(), null, CreateMode.PERSISTENT);
            } finally {
                if (zk != null)
                    zk.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setOffset(String group, String topic, int partition, String offset) {
        String path = String.format("/consumers/%s/offsets/%s/%d", group, topic, partition);
//        ZkClient client = getZKClient();
//        ZkUtils.updatePersistentPath(client, path, offset);
//        client.close();
        int retry = 5;
        try {
            ZooKeeper zk = null;
            while (retry > 0) {
                try {
                    zk = getZK();
                    zk.setData(path, offset.getBytes(), -1);
                    retry = 0;
                } catch (Exception e) {
                    retry--;
                    if (retry == 0)
                        throw e;
                } finally {
                    if (zk != null)
                        zk.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteKafkaGroup(String group) {
        String path = String.format("/consumers/%s", group);
        int retry = 5;
        ZooKeeper zk = null;
        try {
            while (retry > 0) {
                try {
                    zk = getZK();
                    ZKUtil.deleteRecursive(zk, path);
                } catch (Exception e) {
                    retry--;
                    if (retry == 0)
                        throw e;
                } finally {
                    if (zk != null)
                        zk.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        setOffset("consumer.group.news.all.offset.track", "topic_rhino_news_bbs_all_v3", 0, "4439364");
        System.out.println("[PROGRAM] Program exited.");
    }
}
