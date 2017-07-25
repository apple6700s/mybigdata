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
public class WbCntHBaseWriter extends BanyanRFieldPutter {
    static final String table = Tables.table(Tables.PH_WBCNT_TBL);

    public WbCntHBaseWriter() {
        super(table);
    }

    public WbCntHBaseWriter(int cacheSize) {
        super(table, cacheSize);
    }

    private static volatile WbCntHBaseWriter _singleton = null;

    public static WbCntHBaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (WbCntHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntHBaseWriter();
                }
            }
        }
        return _singleton;
    }

    public static WbCntHBaseWriter getInstance(int num) {
        if (_singleton == null) {
            synchronized (WbCntHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WbCntHBaseWriter(num);
                }
            }
        }
        return _singleton;
    }

    protected void init() {
        super.init();
        this.setEsWriterHook(WbCntESWriter.getInstance(), WbCnt2RhinoESDocMapper.class);
    }
}
