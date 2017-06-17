package com.datastory.banyan.weibo.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.doc.UserFollowListDocMapper;
import com.datastory.banyan.weibo.doc.WbUser2RhinoESDocMapper;
import com.datastory.banyan.weibo.doc.WbUserHb2ESDocMapper;
import com.datastory.banyan.weibo.es.WbUserESWriter;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Date;
import java.util.List;


/**
 * com.datastory.banyan.weibo.flush_es.WeiboUserFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WeiboUserFlushESMR extends ScanFlushESMR  {
    private String startUpdateDate = null;
    private String endUpdateDate = null;

    public int getReducerNum() {
        return 40;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
        scan.addFamily("r".getBytes());
        scan.addFamily("f".getBytes());

        FilterList filterList = new FilterList();
        if (startUpdateDate != null) {
            SingleColumnValueFilter startFilter = new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startUpdateDate.getBytes());
            filterList.addFilter(startFilter);
        }
        if (endUpdateDate != null) {
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, endUpdateDate.getBytes());
            filterList.addFilter(endFilter);
        }

        if (startUpdateDate != null || endUpdateDate != null)
            scan.setFilter(filterList);

        Job job = buildJob(Tables.table(Tables.PH_WBUSER_TBL), scan, WeiboUserScanMapper.class, WeiboUserFlushReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        WeiboUserFlushESMR mr = new WeiboUserFlushESMR();
        if (args.length >= 1)
            mr.startUpdateDate = args[0];
        if (args.length >= 2) {
            mr.endUpdateDate = args[1];
        }

        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WeiboUserScanMapper extends ScanMapper {
        @Override
        public Params mapDoc(Params hbDoc) {
            return new WbUser2RhinoESDocMapper(hbDoc).map();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (result == null || result.isEmpty())
                return;
            String pk = new String(result.getRow());
            if (pk.length() < 4)
                return;

            Params hbDoc = new ResultRDocMapper(result).map();
            Params esDoc = mapDoc(hbDoc);
            if (esDoc != null && esDoc.get(UserFollowListDocMapper.FOLLOW_LIST_KEY) == null) {
                List<String> followList = new UserFollowListDocMapper(result).map();
                if (CollectionUtil.isNotEmpty(followList))
                    esDoc.put(UserFollowListDocMapper.FOLLOW_LIST_KEY, followList);
                context.write(new Text(routing(pk)), esDoc);
            } else
                System.err.println("[ErrHbDoc] " + hbDoc);
        }
    }

    public static class WeiboUserFlushReducer extends FlushESReducer {
        public boolean isDebug() {
            return false;
        }

        @Override
        public ESWriterAPI getESWriter() {
            return WbUserESWriter.getInstance();
        }
    }
}
