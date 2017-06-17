package com.datastory.banyan.newsforum.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Date;


/**
 * com.datastory.banyan.newsforum.flush_es.NewsForumPostFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class NewsForumPostFlushESMR extends ScanFlushESMR {

    public void run(Filter filter) throws Exception {
        Scan scan = buildAllScan(filter);
//        scan.setStartRow("0e8cae04478c311fffb1ef9805912539".getBytes());
//        scan.setStopRow("0e8cae04478c311fffb1ef980591253a".getBytes());
        Job job = buildJob(Tables.table(Tables.PH_LONGTEXT_POST_TBL), scan, NewsForumPostScanMapper.class, NewsForumPostFlushReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        NewsForumPostFlushESMR mr = new NewsForumPostFlushESMR();


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
    }

    public static class NewsForumPostFlushReducer extends FlushESReducer {

        @Override
        public ESWriter getESWriter() {
            return NewsForumPostESWriter.getInstance();
        }
    }
}
