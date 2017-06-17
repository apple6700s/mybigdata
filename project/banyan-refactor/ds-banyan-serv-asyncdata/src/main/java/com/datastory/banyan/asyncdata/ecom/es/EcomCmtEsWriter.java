package com.datastory.banyan.asyncdata.ecom.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.asyncdata.ecom.es.EcomItemEsWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomCmtEsWriter extends BanyanESWriter {
    public EcomCmtEsWriter() {
        super(Tables.table(Tables.ES_ECOM_IDX), "comment");
    }

    public EcomCmtEsWriter(int bulkNum) {
        super(Tables.table(Tables.ES_ECOM_IDX), "comment", bulkNum);
    }

    private static volatile EcomCmtEsWriter _singleton = null;

    public static EcomCmtEsWriter getInstance() {
        if (_singleton == null) {
            synchronized (EcomCmtEsWriter.class) {
                if (_singleton == null) {
                    _singleton = new EcomCmtEsWriter();
                }
            }
        }
        return _singleton;
    }
}
