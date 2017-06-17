package com.datastory.banyan.asyncdata.ecom.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.asyncdata.ecom.es.EcomItemEsWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomItemEsWriter extends BanyanESWriter {
    public EcomItemEsWriter() {
        super(Tables.table(Tables.ES_ECOM_IDX), "item");
    }

    public EcomItemEsWriter(int bulkNum) {
        super(Tables.table(Tables.ES_ECOM_IDX), "item", bulkNum);
    }

    private static volatile EcomItemEsWriter _singleton = null;

    public static EcomItemEsWriter getInstance() {
        if (_singleton == null) {
            synchronized (EcomItemEsWriter.class) {
                if (_singleton == null) {
                    _singleton = new EcomItemEsWriter();
                }
            }
        }
        return _singleton;
    }
}
