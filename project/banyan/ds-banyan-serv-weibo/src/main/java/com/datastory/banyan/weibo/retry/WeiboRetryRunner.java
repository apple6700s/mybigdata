package com.datastory.banyan.weibo.retry;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.retry.HBaseRetryWriter;
import com.datastory.banyan.retry.RetryRunner;
import com.datastory.banyan.retry.RetryWriter;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.*;
import com.yeezhao.commons.util.AdvCli;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Date;

/**
 * com.datastory.banyan.weibo.retry.WeiboRetryRunner
 *
 * @author lhfcws
 * @since 2017/3/1
 */
public class WeiboRetryRunner extends RetryRunner {
    protected static final String TBL_CMT = Tables.table(Tables.PH_WBCMT_TBL);
    protected static final String TBL_CNT = Tables.table(Tables.PH_WBCNT_TBL);
    protected static final String TBL_USER = Tables.table(Tables.PH_WBUSER_TBL);
    protected static final String IDX_WB = Tables.table(Tables.ES_WB_IDX);
    protected static final String IDX_WBCMT = Tables.table(Tables.ES_WBCMT_IDX);
    protected static final String IDX_WBCMT2 = Tables.ES_WBCMT_IDX;

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return WeiboRetryMapper.class;
    }

    @Override
    protected boolean filterPath(String path) {
        return path.endsWith(TBL_CNT) || path.endsWith(TBL_USER) || path.endsWith(TBL_CMT)
                || path.contains("." + IDX_WB + ".") || path.contains("." + Tables.ES_WB_IDX + ".")
                || path.contains("." + IDX_WBCMT + ".") || path.contains("." + Tables.ES_WBCMT_IDX + ".")
                ;
    }

    public static class WeiboRetryMapper extends RetryWriterMapper {
        protected ESWriter createEsWriter() {
            if (table.equals(IDX_WB)) {
                if (type.equals("weibo"))
                    return WbCntESWriter.getInstance(table, 3000);
                else
                    return WbUserESWriter.getInstance(table, 2000);
            } else if (table.equals(IDX_WBCMT)) {
                if (type.equals("weibo"))
                    return RhinoCmtWbESWriter.getInstance(table);
                else
                    return RhinoCmtESWriter.getInstance(table);
            } else {
                if (type.equals("weibo"))
                    return CmtWbESWriter.getInstance(table);
                else
                    return CmtESWriter.getInstance(table);
            }
        }

        @Override
        protected RetryWriter createHbaseRetryWriter() {
            HBaseRetryWriter hrw = new HBaseRetryWriter(table);
            if (table.equals(TBL_CNT))
                hrw.setEsWriterHook(WbCntESWriter.getInstance(), WbCnt2RhinoESDocMapper.class);
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

        RetryRunner retryRunner = new WeiboRetryRunner();
        System.out.println("Start " + retryRunner.getClass().getSimpleName());
        AdvCli.initRunner(args, retryRunner.getClass().getSimpleName(), retryRunner);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
