package com.datastory.banyan.newsforum.retry;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;
import com.datastory.banyan.retry.HBaseRetryWriter;
import com.datastory.banyan.retry.RetryRunner;
import com.datastory.banyan.retry.RetryWriter;
import com.yeezhao.commons.util.AdvCli;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Date;

/**
 * com.datastory.banyan.newsforum.retry.NewsForumRetryRunner
 *
 * @author lhfcws
 * @since 2017/3/1
 */
public class NewsForumRetryRunner extends RetryRunner {
    protected static final String TBL_POST = Tables.table(Tables.PH_LONGTEXT_POST_TBL);
    protected static final String TBL_CMT = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
    protected static final String IDX_LT = Tables.table(Tables.ES_LTEXT_IDX);

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return NewsForumRetryMapper.class;
    }

    @Override
    protected boolean filterPath(String path) {
        return path.endsWith(TBL_POST) || path.endsWith(TBL_CMT) || path.contains("." + IDX_LT + ".");
    }

    public static class NewsForumRetryMapper extends RetryWriterMapper {

        protected ESWriter createEsWriter() {
            if (type.equals("post"))
                return NewsForumPostESWriter.getInstance();
            else
                return NewsForumCmtESWriter.getInstance();
        }

        @Override
        protected RetryWriter createHbaseRetryWriter() {
            HBaseRetryWriter hrw = new HBaseRetryWriter(table);
            if (table.equals(TBL_POST))
                hrw.setEsWriterHook(NewsForumPostESWriter.getInstance(), NFPostHb2ESDocMapper.class);
            else if (table.equals(TBL_CMT))
                hrw.setEsWriterHook(NewsForumCmtESWriter.getInstance(), NFCmtHb2ESDocMapper.class);
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

        RetryRunner retryRunner = new NewsForumRetryRunner();
        System.out.println("Start " + retryRunner.getClass().getSimpleName());
        AdvCli.initRunner(args, retryRunner.getClass().getSimpleName(), retryRunner);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
