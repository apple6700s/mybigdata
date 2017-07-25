package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;

/**
 * com.datastory.banyan.wechat.hbase.WxCntHBaseWriter
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class WxCntHBaseWriter extends BanyanRFieldPutter {
    static String table = Tables.table(Tables.PH_WXCNT_TBL);

    public WxCntHBaseWriter() {
        super(table);
    }

    public WxCntHBaseWriter(int cacheSize) {
        super(table, cacheSize);
    }

    protected void init() {
        super.init();
        this.setEsWriterHook(WxCntESWriter.getInstance(), WxCntHb2ESDocMapper.class);
    }

    private static volatile WxCntHBaseWriter _singleton = null;

    public static WxCntHBaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (WxCntHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxCntHBaseWriter();
                }
            }
        }
        return _singleton;
    }

    public static WxCntHBaseWriter getInstance(int num) {
        if (_singleton == null) {
            synchronized (WxCntHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxCntHBaseWriter(num);
                }
            }
        }
        return _singleton;
    }
}
