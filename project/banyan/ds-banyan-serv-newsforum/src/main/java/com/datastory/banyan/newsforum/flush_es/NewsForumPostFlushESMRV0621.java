package com.datastory.banyan.newsforum.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Date;


/**
 * com.datastory.banyan.newsforum.flush_es.NewsForumPostFlushESMRV0621
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class NewsForumPostFlushESMRV0621 extends ScanFlushESMR {

    public void run(Filter filter) throws Exception {
        Scan scan = buildAllScan(filter);
        Job job = buildJob(Tables.table(Tables.PH_LONGTEXT_POST_TBL), scan, NewsForumPostScanMapper.class, NewsForumPostFlushReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        NewsForumPostFlushESMRV0621 mr = new NewsForumPostFlushESMRV0621();


        FilterList filterList = new FilterList();
        if (args.length >= 1) {
            String startUpdateDate = args[0];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }

        if (args.length >= 2) {
            String endUpdateDate = args[1];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, endUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }

        filterList.addFilter(new SingleColumnValueFilter("r".getBytes(), "taskId".getBytes(), CompareFilter.CompareOp.EQUAL, "116833".getBytes()));
        if (filterList.getFilters().isEmpty())
            mr.run(null);
        else
            mr.run(filterList);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class NewsForumPostScanMapper extends ScanMapper {
        @Override
        public Params mapDoc(Params hbDoc) {
            return new NFPostHb2ESDocMapper(hbDoc).map();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (value == null || value.isEmpty()) {
                context.getCounter(ROW.FILTER).increment(1);
                return;
            }
            String pk = new String(value.getRow());
            if (pk.length() < 4) {
                context.getCounter(ROW.FILTER).increment(1);
                return;
            }

            String taskId = HBaseUtils.getValue(value, "r".getBytes(), "taskId".getBytes());
            if (taskId != null) {
                Params hbDoc = new ResultRDocMapper(value).map();

                Params esDoc = mapDoc(hbDoc);

                if (esDoc != null) {
                    String prefix = pk.substring(0, 3);
                    context.write(new Text(prefix), esDoc);
                    context.getCounter(ROW.SHUFFLE).increment(1);
                } else {
                    context.getCounter(ROW.ERROR).increment(1);
                    System.err.println("[ErrHbDoc] " + hbDoc);
                }
            } else
                context.getCounter(ROW.FILTER).increment(1);
        }
    }

    public static class NewsForumPostFlushReducer extends FlushESReducer {

        @Override
        public ESWriter getESWriter() {
            return NewsForumPostESWriter.getInstance().setSyncMode();
        }
    }
}
