package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.WbCntESWriter;

/**
 * com.datastory.banyan.weibo.hbase.WbCntHBaseWriter
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class WbUserHBaseWriter extends BanyanRFieldPutter {
    static final String table = Tables.table(Tables.PH_WBUSER_TBL);

    public WbUserHBaseWriter() {
        super(table);
    }

    public WbUserHBaseWriter(int cacheSize) {
        super(table, cacheSize);
    }

    private static volatile WbUserHBaseWriter _singleton = null;

    public static WbUserHBaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (WbUserHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserHBaseWriter();
                }
            }
        }
        return _singleton;
    }

    public static WbUserHBaseWriter getInstance(int num) {
        if (_singleton == null) {
            synchronized (WbUserHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbUserHBaseWriter(num);
                }
            }
        }
        return _singleton;
    }

    protected void init() {
        super.init();
    }
}
