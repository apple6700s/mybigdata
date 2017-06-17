package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;

/**
 * com.datastory.banyan.wechat.hbase.WxContentHBaseReader
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WxContentHBaseReader extends HBaseReader {
    private static final String TABLE = Tables.table(Tables.PH_WXCNT_TBL);

    private static volatile WxContentHBaseReader _singleton = null;

    public static WxContentHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (WxContentHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new WxContentHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
