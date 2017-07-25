package com.datastory.banyan.weibo.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.weibo.analyz.SelfContentExtractor;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.WbCntESWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
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


/**
 * com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR
 * <p>
 * old es schema flush
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WeiboContentFlushESMR1 extends ScanFlushESMR {
    private String startUpdateDate = null;
    private String endUpdateDate = null;

    @Override
    public String toString() {
        return startUpdateDate + " ~ " + endUpdateDate;
    }

    public int getReducerNum() {
        return 50;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
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

        System.out.println("[CONDITION] " + startUpdateDate + " ~ " + endUpdateDate);
        System.out.println("[SCAN] " + scan);
//        scan.setStartRow("0001090925883987".getBytes()).setStopRow("0002011103092332050234".getBytes());
        String table = Tables.table(Tables.PH_WBCNT_TBL);
        Job job = buildJob(table, scan, WeiboContentScanMapper.class, WeiboContentFlushReducer.class);
        job.setJobName(table + ": " + startUpdateDate + "-" + endUpdateDate);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        WeiboContentFlushESMR1 mr = new WeiboContentFlushESMR1();
        if (args.length >= 1)
            mr.startUpdateDate = args[0];
        if (args.length >= 2) {
            mr.endUpdateDate = args[1];
        }

        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WeiboContentScanMapper extends ScanMapper {

        @Override
        public Params mapDoc(Params hbDoc) {
            return new WbCnt2RhinoESDocMapper(hbDoc).map();
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

            Params hbDoc = new ResultRDocMapper(value).map();
            if (!StringUtil.isNullOrEmpty(hbDoc.getString("content"))) {
                String selfContent = SelfContentExtractor.extract(hbDoc.getString("content"));
                if (!StringUtil.isNullOrEmpty(selfContent)) {
                    hbDoc.put("self_content", selfContent);
                    hbDoc.put("self_content_len", selfContent.length());
                }
            }

            Params esDoc = mapDoc(hbDoc);

            if (esDoc != null) {
                context.write(new Text(routing(pk)), esDoc);
                context.getCounter(ROW.SHUFFLE).increment(1);
            } else {
                context.getCounter(ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + hbDoc);
            }
        }
    }

    public static class WeiboContentFlushReducer extends FlushESReducer {
        @Override
        public boolean isDebug() {
            return false;
        }

        @Override
        public ESWriterAPI getESWriter() {
            return WbCntESWriter.getFlushInstance(Tables.table(Tables.ES_WB_IDX), 5000).setSyncMode();
        }
    }
}
