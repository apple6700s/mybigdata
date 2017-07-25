package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;

/**
 * com.datastory.banyan.wechat.hbase.WxMPHBaseReader
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxMPHBaseReader extends HBaseReader {
    private static final String TABLE = Tables.table(Tables.PH_WXMP_TBL);

    private static volatile WxMPHBaseReader _singleton = null;

    public static WxMPHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (WxMPHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new WxMPHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
