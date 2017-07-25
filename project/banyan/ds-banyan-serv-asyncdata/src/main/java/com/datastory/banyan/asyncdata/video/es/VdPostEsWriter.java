package com.datastory.banyan.asyncdata.video.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.asyncdata.video.es.VdPostEsWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class VdPostEsWriter extends BanyanESWriter {
    public VdPostEsWriter() {
        super(Tables.table(Tables.ES_VIDEO_IDX), "post");
    }

    public VdPostEsWriter(int bulkNum) {
        super(Tables.table(Tables.ES_VIDEO_IDX), "post", bulkNum);
    }

    private static volatile VdPostEsWriter _singleton = null;

    public static VdPostEsWriter getInstance() {
        if (_singleton == null) {
            synchronized (VdPostEsWriter.class) {
                if (_singleton == null) {
                    _singleton = new VdPostEsWriter();
                }
            }
        }
        return _singleton;
    }
}
