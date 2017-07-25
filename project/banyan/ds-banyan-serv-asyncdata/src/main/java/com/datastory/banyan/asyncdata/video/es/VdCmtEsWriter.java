package com.datastory.banyan.asyncdata.video.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.asyncdata.video.es.VdPostEsWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class VdCmtEsWriter extends BanyanESWriter {
    public VdCmtEsWriter() {
        super(Tables.table(Tables.ES_VIDEO_IDX), "comment");
    }

    public VdCmtEsWriter(int bulkNum) {
        super(Tables.table(Tables.ES_VIDEO_IDX), "comment", bulkNum);
    }

    private static volatile VdCmtEsWriter _singleton = null;

    public static VdCmtEsWriter getInstance() {
        if (_singleton == null) {
            synchronized (VdCmtEsWriter.class) {
                if (_singleton == null) {
                    _singleton = new VdCmtEsWriter();
                }
            }
        }
        return _singleton;
    }
}
