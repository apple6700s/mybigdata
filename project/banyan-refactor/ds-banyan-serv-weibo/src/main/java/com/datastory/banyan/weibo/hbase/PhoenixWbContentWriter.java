package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.banyan.utils.FactoryFunc;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.WbCntESWriter;
import com.datastory.banyan.weibo.kafka.WeiboContentKafkaProducer;

import java.sql.SQLException;

/**
 * com.datastory.banyan.weibo.hbase.PhoenixWbContentWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PhoenixWbContentWriter extends PhoenixWriter {
    protected static final String TABLE = Tables.table(Tables.PH_WBCNT_TBL);

    private static volatile PhoenixWbContentWriter _singleton = null;

    public static PhoenixWbContentWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWbContentWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbContentWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixWbContentWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixWbContentWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWbContentWriter(num);
                }
            }
        return _singleton;
    }

    public PhoenixWbContentWriter() {
        super(5000);
    }

    public PhoenixWbContentWriter(int cacheSize) {
        super(cacheSize);
    }

    @Override
    protected void init() {
        super.init();

        this.setEsWriterHook(WbCntESWriter.getInstance(), WbCnt2RhinoESDocMapper.class);
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
