package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxMPESWriter;

/**
 * com.datastory.banyan.wechat.hbase.PhoenixWxMPWriter
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class PhoenixWxMPWriter extends PhoenixWriter {
    private static volatile PhoenixWxMPWriter _singleton = null;

    public static PhoenixWxMPWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWxMPWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWxMPWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixWxMPWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixWxMPWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWxMPWriter(num);
                }
            }
        return _singleton;
    }

    private PhoenixWxMPWriter() {
        this(3000);
    }

    public PhoenixWxMPWriter(int cacheSize) {
        super(cacheSize);
    }

    @Override
    protected void init() {
        super.init();
        this.setEsWriterHook(WxMPESWriter.getInstance(), WxMPHb2ESDocMapper.class);
//        this.setEsWriterHook(WxMPESWriter.createSimpleWriterFactory(cacheSize), WxMPHb2ESDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_WXMP_TBL);
    }
}
