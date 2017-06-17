package com.datastory.banyan.newsforum.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.*;
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
 * com.datastory.banyan.newsforum.flush_es.NewsForumCmtFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class NewsForumCmtFlushESMR extends ScanFlushESMR {

    public void run(Filter filter) throws Exception {
        Scan scan = buildAllScan(filter);
//        scan.setStartRow("5c26685bd1de12aec84562bf80e2ab0d".getBytes());
//        scan.setStopRow("5c26685bd1de12aec84562bf80e2ab0e".getBytes());
        Job job = buildJob(Tables.table(Tables.PH_LONGTEXT_CMT_TBL), scan, NewsForumCmtScanMapper.class, NewsForumCmtFlushReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        NewsForumCmtFlushESMR mr = new NewsForumCmtFlushESMR();

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

    public static class NewsForumCmtScanMapper extends ScanMapper {
        @Override
        public Params mapDoc(Params hbDoc) {
            return new NFCmtHb2ESDocMapper(hbDoc).map();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (value == null || value.isEmpty())
                return;
            Params hbDoc = new ResultRDocMapper(value).map();
            Params esDoc = mapDoc(hbDoc);
            String pk = new String(value.getRow());
            if (pk.length() < 4)
                return;

            String parentId = esDoc.getString("_parent");

            if (esDoc != null)
                context.write(new Text(parentId), esDoc);
//            System.out.println("[DEBUG] hb " + hbDoc);
//            System.out.println("[DEBUG] es " + esDoc);
        }
    }

    public static class NewsForumCmtFlushReducer extends FlushESReducer {
        HTableInterfacePool hpool = null;
        static byte[] R = "r".getBytes();
        static byte[] CNT = "content".getBytes();
        static byte[] ALLCNT = "all_content".getBytes();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            hpool = HTableInterfacePool.getInstance();
        }

        @Override
        public ESWriter getESWriter() {
            return NewsForumCmtESWriter.getInstance();
        }

        @Override
        protected void reduce(Text key, Iterable<Params> values, Context context) throws IOException, InterruptedException {
            String parentId = key.toString();
            StringBuilder allContentSB = new StringBuilder();

            ESWriter writer = getESWriter();
            long size = 0;
            for (Params p : values) {
                if (size == 0)
                    System.out.println("[DEBUG] " + p);
                size++;
                if (hpool != null && size < 100) {
                    if (!StringUtils.isEmpty(p.getString("content"))) {
                        allContentSB.append(p.getString("content")).append(" ");
                    }
                }

                try {
                    writer.write(p);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            context.getCounter(ROW.WRITE).increment(size);

            if (hpool != null) {
                HTableInterface hti = null;
                try {
                    hti = hpool.getTable(Tables.table(Tables.PH_LONGTEXT_POST_TBL));
                    Get get = new Get(parentId.getBytes());
                    get.addColumn(R, CNT);
                    Result result = hti.get(get);
                    String content = HBaseUtils.getValue(result, R, CNT);
                    allContentSB.append(content);

                    Put put = new Put(parentId.getBytes());
                    put.addColumn(R, ALLCNT, allContentSB.toString().getBytes());
                    hti.put(put);
                    hti.flushCommits();
                } catch (Exception e) {
                    ErrorUtil._LOG.error(e);
                } finally {
                    hpool.close(hti);
                }
            }
        }
    }
}
