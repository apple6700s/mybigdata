package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;

/**
 * com.datastory.banyan.weibo.hbase.PhoenixWbCommentWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PhoenixWbCommentWriterV1 extends PhoenixWriter {
    protected static final String TABLE = Tables.table(Tables.PH_WBCMT_TBL);

    private static volatile PhoenixWbCommentWriterV1 _singleton = null;

    public static PhoenixWbCommentWriterV1 getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWbCommentWriterV1.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbCommentWriterV1();
                }
            }
        return _singleton;
    }

    private PhoenixWbCommentWriterV1() {
        super(3000);
    }

    @Override
    public String getTable() {
        return "DS_BANYAN_WEIBO_COMMENT_V1";
    }
}
