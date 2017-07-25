package com.datastory.banyan.asyncdata.ecom.hbase;

import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomItemDocMapper;
import com.datastory.banyan.asyncdata.ecom.es.EcomItemEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;

/**
 * com.datastory.banyan.asyncdata.ecom.hbase.EcomItemHbaseWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomItemHbaseWriter extends BanyanRFieldPutter {
    private static volatile EcomItemHbaseWriter _singleton = null;

    public static EcomItemHbaseWriter getInstance() {
        if (_singleton == null) {
            synchronized (EcomItemHbaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new EcomItemHbaseWriter();
                }
            }
        }
        return _singleton;
    }
    
    public EcomItemHbaseWriter() {
        super(Tables.table(Tables.PH_ECOM_ITEM_TBL));
        setEsWriterHook(EcomItemEsWriter.getInstance(), Hb2EsEcomItemDocMapper.class);
    }

    public EcomItemHbaseWriter(int cacheSize) {
        super(Tables.table(Tables.PH_ECOM_ITEM_TBL), cacheSize);
        setEsWriterHook(EcomItemEsWriter.getInstance(), Hb2EsEcomItemDocMapper.class);
    }
}
