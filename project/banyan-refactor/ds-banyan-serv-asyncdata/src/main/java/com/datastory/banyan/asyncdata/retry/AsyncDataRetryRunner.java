package com.datastory.banyan.asyncdata.retry;

import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomCommentDocMapper;
import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomItemDocMapper;
import com.datastory.banyan.asyncdata.ecom.es.EcomCmtEsWriter;
import com.datastory.banyan.asyncdata.ecom.es.EcomItemEsWriter;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdCommentDocMapper;
import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdPostDocMapper;
import com.datastory.banyan.asyncdata.video.es.VdCmtEsWriter;
import com.datastory.banyan.asyncdata.video.es.VdPostEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.retry.HBaseRetryWriter;
import com.datastory.banyan.retry.RetryRunner;
import com.datastory.banyan.retry.RetryWriter;
import com.yeezhao.commons.util.AdvCli;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Date;

/**
 * com.datastory.banyan.asyncdata.retry.AsyncDataRetryRunner
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class AsyncDataRetryRunner extends RetryRunner {
    protected static final String TBL_VD_POST = Tables.table(Tables.PH_VIDEO_POST_TBL);
    protected static final String TBL_VD_CMT = Tables.table(Tables.PH_VIDEO_CMT_TBL);
    protected static final String TBL_ECOM_ITEM = Tables.table(Tables.PH_ECOM_ITEM_TBL);
    protected static final String TBL_ECOM_CMT = Tables.table(Tables.PH_ECOM_CMT_TBL);
    protected static final String IDX_ECOM = Tables.table(Tables.ES_ECOM_IDX);
    protected static final String IDX_VD = Tables.table(Tables.ES_VIDEO_IDX);

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return AsyncDataRetryMapper.class;
    }

    @Override
    protected boolean filterPath(String path) {
        return
                path.contains(TBL_VD_POST) || path.contains(TBL_VD_CMT) || path.contains(IDX_VD)
                        || path.contains(TBL_ECOM_ITEM) || path.contains(TBL_ECOM_CMT) || path.contains(IDX_ECOM)
                ;
    }

    public static class AsyncDataRetryMapper extends RetryWriterMapper {
        protected ESWriter createEsWriter() {
            if (type.equals("comment")) {
                if (table.equals(IDX_VD)) {
                    return VdCmtEsWriter.getInstance();
                } else if (table.equals(IDX_ECOM))
                    return EcomCmtEsWriter.getInstance();
            } else if (type.equals("post") && table.equals(IDX_VD)) {
                return VdPostEsWriter.getInstance();
            } else if (type.equals("item") && table.equals(IDX_ECOM)) {
                return VdPostEsWriter.getInstance();
            }
            return null;
        }

        @Override
        protected RetryWriter createHbaseRetryWriter() {
            HBaseRetryWriter hrw = new HBaseRetryWriter(table);
            if (table.equals(TBL_VD_POST))
                hrw.setEsWriterHook(VdPostEsWriter.getInstance(), Hb2EsVdPostDocMapper.class);
            else if (table.equals(TBL_VD_CMT))
                hrw.setEsWriterHook(VdCmtEsWriter.getInstance(), Hb2EsVdCommentDocMapper.class);
            else if (table.equals(TBL_ECOM_ITEM))
                hrw.setEsWriterHook(EcomItemEsWriter.getInstance(), Hb2EsEcomItemDocMapper.class);
            else if (table.equals(TBL_ECOM_CMT))
                hrw.setEsWriterHook(EcomCmtEsWriter.getInstance(), Hb2EsEcomCommentDocMapper.class);
            return hrw;
        }
    }

    /**
     * MAIN
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        RetryRunner retryRunner = new AsyncDataRetryRunner();
        System.out.println("Start " + retryRunner.getClass().getSimpleName());
        AdvCli.initRunner(args, retryRunner.getClass().getSimpleName(), retryRunner);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
