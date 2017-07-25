package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxMPESWriter;

/**
 * com.datastory.banyan.wechat.hbase.WxMpHBaseWriter
 *
 * @author lhfcws
 * @since 2017/2/17
 */
public class WxMpHBaseWriter extends BanyanRFieldPutter {
    static String table = Tables.table(Tables.PH_WXMP_TBL);

    public WxMpHBaseWriter() {
        super(table);
    }

    public WxMpHBaseWriter(int cacheSize) {
        super(table, cacheSize);
    }

    protected void init() {
        super.init();
        this.setEsWriterHook(WxMPESWriter.getInstance(), WxMPHb2ESDocMapper.class);
    }

    private static volatile WxMpHBaseWriter _singleton = null;

    public static WxMpHBaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (WxMpHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxMpHBaseWriter();
                }
            }
        }
        return _singleton;
    }

    public static WxMpHBaseWriter getInstance(int num) {
        if (_singleton == null) {
            synchronized (WxMpHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new WxMpHBaseWriter(num);
                }
            }
        }
        return _singleton;
    }
}
