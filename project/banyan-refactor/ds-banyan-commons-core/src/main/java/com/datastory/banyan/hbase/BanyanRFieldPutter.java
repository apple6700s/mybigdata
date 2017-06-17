package com.datastory.banyan.hbase;

import com.datastory.banyan.hbase.hooks.*;

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
}
