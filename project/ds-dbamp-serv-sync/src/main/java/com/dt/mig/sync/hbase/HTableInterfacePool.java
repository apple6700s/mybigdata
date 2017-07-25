package com.dt.mig.sync.hbase;

import com.ds.dbamp.core.base.cfg.DbampConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * com.datastory.banyan.hbase.HTableInterfacePool
 * <p>
 * 使用HConnection来获取HTableInterface，线程安全。
 *
 * @author lhfcws
 * @since 2016/10/18
 */
public class HTableInterfacePool implements Serializable {
    protected static Logger LOG = Logger.getLogger(HTableInterfacePool.class);
    private static volatile HTableInterfacePool _singleton = null;

    public static HTableInterfacePool getInstance() {
        if (_singleton == null) {
            synchronized (HTableInterfacePool.class) {
                if (_singleton == null) {
                    try {
                        _singleton = new HTableInterfacePool();
                        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    _singleton.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return _singleton;
    }

    public static HTableInterface get(String tbl) throws IOException {
        return getInstance().getTable(tbl);
    }

    /**
     * 强烈要求用完HTable进行关闭
     *
     * @param hti
     * @throws java.io.IOException
     */
    public static void close(HTableInterface hti) throws IOException {
        if (hti != null) hti.close();
    }

    protected HConnection hConn;

    public HTableInterfacePool() throws IOException {
        Configuration config = DbampConfiguration.getInstance();
        System.out.println();
        hConn = HConnectionManager.createConnection(config);
    }

    public HTableInterface getTable(String tbl) throws IOException {
        return hConn.getTable(tbl);
    }

    /**
     * 除非进程关闭，一般不建议执行
     *
     * @throws java.io.IOException
     */
    public void close() throws IOException {
        if (hConn != null) hConn.close();
    }
}
