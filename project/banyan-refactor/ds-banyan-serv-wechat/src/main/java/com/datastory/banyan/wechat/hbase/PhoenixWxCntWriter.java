package com.datastory.banyan.wechat.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;

/**
 * com.datastory.banyan.wechat.hbase.PhoenixWxCntWriter
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class PhoenixWxCntWriter extends PhoenixWriter {
    private static volatile PhoenixWxCntWriter _singleton = null;

    public static PhoenixWxCntWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixWxCntWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWxCntWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixWxCntWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixWxCntWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixWxCntWriter(num);
                }
            }
        return _singleton;
    }

    public PhoenixWxCntWriter() {
        this(1000);
    }

    public PhoenixWxCntWriter(int cacheSize) {
        super(cacheSize);

    }

    @Override
    protected void init() {
        super.init();
        this.setEsWriterHook(WxCntESWriter.getInstance(), WxCntHb2ESDocMapper.class);
//        this.setEsWriterHook(WxCntESWriter.createSimpleWriterFactory(cacheSize), WxCntHb2ESDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_WXCNT_TBL);
    }
}
