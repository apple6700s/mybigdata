package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;

/**
 * com.datastory.banyan.weibo.hbase.WbCntHBaseReader
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WbCntHBaseReader extends HBaseReader {
    private static volatile WbCntHBaseReader _singleton = null;

    public static WbCntHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (WbCntHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new WbCntHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_WBCNT_TBL);
    }
}
