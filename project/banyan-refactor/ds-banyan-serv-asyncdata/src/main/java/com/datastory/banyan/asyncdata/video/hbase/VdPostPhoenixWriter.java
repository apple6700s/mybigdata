package com.datastory.banyan.asyncdata.video.hbase;

import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdPostDocMapper;
import com.datastory.banyan.asyncdata.video.es.VdPostEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;

/**
 * com.datastory.banyan.asyncdata.video.hbase.VdPostPhoenixWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class VdPostPhoenixWriter extends PhoenixWriter {
    private static volatile VdPostPhoenixWriter _singleton = null;

    public static VdPostPhoenixWriter getInstance() {
        if (_singleton == null) {
            synchronized (VdPostPhoenixWriter.class) {
                if (_singleton == null) {
                    _singleton = new VdPostPhoenixWriter();
                }
            }
        }
        return _singleton;
    }

    public VdPostPhoenixWriter() {
        setEsWriterHook(VdPostEsWriter.getInstance(), Hb2EsVdPostDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_VIDEO_POST_TBL);
    }
}
