package com.datastory.banyan.asyncdata.ecom.hbase;

import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomCommentDocMapper;
import com.datastory.banyan.asyncdata.ecom.es.EcomCmtEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;

/**
 * com.datastory.banyan.asyncdata.ecom.hbase.EcomCommentPhoenixWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class EcomCommentPhoenixWriter extends PhoenixWriter {
    private static volatile EcomCommentPhoenixWriter _singleton = null;

    public static EcomCommentPhoenixWriter getInstance() {
        if (_singleton == null) {
            synchronized (EcomCommentPhoenixWriter.class) {
                if (_singleton == null) {
                    _singleton = new EcomCommentPhoenixWriter();
                }
            }
        }
        return _singleton;
    }

    public EcomCommentPhoenixWriter() {
        setEsWriterHook(EcomCmtEsWriter.getInstance(), Hb2EsEcomCommentDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_ECOM_CMT_TBL);
    }
}
