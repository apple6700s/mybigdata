package com.datastory.banyan.wechat.retry;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.retry.HBaseRetryWriter;
import com.datastory.banyan.retry.RetryRunner;
import com.datastory.banyan.retry.RetryWriter;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;
import com.datastory.banyan.wechat.es.WxMPESWriter;
import com.yeezhao.commons.util.AdvCli;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Date;

/**
 * com.datastory.banyan.wechat.retry.WechatRetryRunner
 *
 * @author lhfcws
 * @since 2017/3/1
 */
public class WechatRetryRunner extends RetryRunner {
    protected static final String TBL_CNT = Tables.table(Tables.PH_WXCNT_TBL);
    protected static final String TBL_MP = Tables.table(Tables.PH_WXMP_TBL);
    protected static final String IDX_WX = Tables.table(Tables.ES_WECHAT_IDX);

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return WechatRetryMapper.class;
    }

    @Override
    protected boolean filterPath(String path) {
        return path.endsWith(TBL_CNT) || path.endsWith(TBL_MP) || path.contains("." + IDX_WX + ".");
    }

    public static class WechatRetryMapper extends RetryWriterMapper {
        protected ESWriter createEsWriter() {
            if (type.equals("mp"))
                return WxMPESWriter.getInstance();
            else
                return WxCntESWriter.getInstance();
        }

        @Override
        protected RetryWriter createHbaseRetryWriter() {
            HBaseRetryWriter hrw = new HBaseRetryWriter(table);
            if (table.equals(TBL_CNT))
                hrw.setEsWriterHook(WxCntESWriter.getInstance(), WxCntHb2ESDocMapper.class);
            else if (table.equals(TBL_MP))
                hrw.setEsWriterHook(WxMPESWriter.getInstance(), WxMPHb2ESDocMapper.class);
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

        RetryRunner retryRunner = new WechatRetryRunner();
        System.out.println("Start " + retryRunner.getClass().getSimpleName());
        AdvCli.initRunner(args, retryRunner.getClass().getSimpleName(), retryRunner);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
