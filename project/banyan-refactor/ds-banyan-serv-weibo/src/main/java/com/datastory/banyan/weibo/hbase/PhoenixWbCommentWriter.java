package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.weibo.kafka.WeiboContentKafkaProducer;

/**
 * com.datastory.banyan.weibo.hbase.PhoenixWbCommentWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PhoenixWbCommentWriter extends PhoenixWriter {
    protected static final String TABLE = Tables.table(Tables.PH_WBCMT_TBL);

    private static volatile PhoenixWbCommentWriter _singleton = null;

    public static PhoenixWbCommentWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWbCommentWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbCommentWriter();
                }
            }
        return _singleton;
    }

    private PhoenixWbCommentWriter() {
        super(10000);
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
