package com.dt.mig.sync.hbase;

import com.yeezhao.commons.util.Reference;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 单独对某些字段进行put
 * <p>
 * com.datastory.banyan.hbase.RFieldPutter
 *
 * @author lhfcws
 * @since 16/11/15
 */

public class RFieldPutter implements Serializable {
    protected static Logger LOG = Logger.getLogger(RFieldPutter.class);

    public static final int DEFAULT_RETRY = 3;
    private List<Put> cache = new LinkedList<>();
    private String table;
    private byte[] family = "r".getBytes();
    private int cacheSize = 1000;

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public RFieldPutter(String table) {
        this.table = table;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
    }

    public String getTable() {
        return table;
    }

    public int batchWrite(Map<String, ? extends Object> p) throws IOException {
        String pk = (String) p.get("pk");
        if (StringUtil.isNullOrEmpty(pk)) throw new IOException("pk should not be null");

        Put put = new Put(pk.getBytes());
        for (Map.Entry<String, ? extends Object> e : p.entrySet()) {
            if (e.getValue() == null) continue;
            String v = String.valueOf(e.getValue());
            put.addColumn(getFamily(), e.getKey().getBytes(), v.getBytes());
        }
        return batchWrite(put);
    }

    public int batchWrite(Put put) throws IOException {
        synchronized (cache) {
            cache.add(put);
        }
        if (cache.size() > getCacheSize()) {
            synchronized (this) {
                if (cache.size() > getCacheSize()) return flush();
            }
        }
        return 0;
    }


    public int flush() throws IOException {
        final List<Put> clist = new ArrayList<>();
        synchronized (cache) {
            clist.addAll(cache);
            cache.clear();
        }
        final Reference<Integer> ref = new Reference<>(0);
        if (clist.size() > 0) {
            HTableInterface hTable = HTableInterfacePool.getInstance().getTable(getTable());
            try {

                int retry = DEFAULT_RETRY;
                while (true) {
                    try {
                        retry--;
                        hTable.put(clist);
                        break;
                    } catch (Exception e) {
                        if (retry <= 0) throw e;
                    }
                }

                ref.setValue(clist.size());
                return ref.getValue();
            } catch (Exception e) {
                ref.setValue(-1);
                return ref.getValue();
            } finally {
                hTable.close();
            }
        } else return ref.getValue();
    }
}
