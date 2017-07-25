package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.hbase.hooks.HBaseMain2Hook;
import com.datastory.banyan.hbase.hooks.HBaseMonitor2Hook;
import com.datastory.banyan.hbase.hooks.HBaseRetryLog2Hook;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.Map;

/**
 * com.datastory.banyan.hbase.BanyanRFieldPutter
 *
 * @author lhfcws
 * @since 2017/2/16
 */
public class BanyanRFieldPutter extends RFieldPutter {
    public BanyanRFieldPutter(String table) {
        super(table);
        initHooks();
        init();
    }

    public BanyanRFieldPutter(String table, int cacheSize) {
        this(table);
        setCacheSize(cacheSize);
    }

    protected void init() {}

    protected void initHooks() {
        this.getHooks().add(new HBaseMain2Hook(
                new HBaseMonitor2Hook(getTable()),
                new HBaseRetryLog2Hook(getTable())
        ));
    }

    public int batchWrite(Map<String, ? extends Object> p) throws IOException {
        String pk = (String) p.remove("pk");
        if (StringUtil.isNullOrEmpty(pk))
            throw new IOException("pk should not be null");

        Put put = new Put(pk.getBytes());
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ? extends Object> e : p.entrySet()) {
            if (e.getValue() == null ) continue;
            String v = String.valueOf(e.getValue());
            if (RhinoETLConsts.HB_M_FIELDS_SET.contains(e.getKey())) {
                put.addColumn(RhinoETLConsts.M, e.getKey().getBytes(), now, v.getBytes());
            } else {
                put.addColumn(getFamily(), e.getKey().getBytes(), v.getBytes());
            }
        }
        return batchWrite(put);
    }
}
