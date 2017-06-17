package com.datastory.banyan.weibo.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.weibo.doc.RhinoCmtWeiboEsDocMapper;
import com.datastory.banyan.weibo.es.RhinoCmtWbESWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
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
 * com.datastory.banyan.weibo.flush_es.CmtWeiboFlushESMR
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class CmtWeiboFlushESMR extends ScanFlushESMR {
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
//        String table = "dt.rhino.weibo.comment";
        String table = Tables.table(Tables.PH_WBCMT_TBL);
        Job job = buildJob(table, scan, CmtWeiboScanMapper.class, WeiboContentFlushReducer.class);
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.memory.mb", "2048");
        job.setJobName(table + ": " + startUpdateDate + "-" + endUpdateDate);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        CmtWeiboFlushESMR mr = new CmtWeiboFlushESMR();
        if (args.length >= 1)
            mr.startUpdateDate = args[0];
        if (args.length >= 2) {
            mr.endUpdateDate = args[1];
        }

        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class CmtWeiboScanMapper extends ScanFlushESMR.ScanMapper {
        HBaseReader cntReader;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            cntReader = new HBaseReader(Tables.table(Tables.PH_WBCNT_TBL));
        }

        @Override
        public Params mapDoc(Params hbDoc) {
            try {
                return new RhinoCmtWeiboEsDocMapper(hbDoc).fillUser().map();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (value == null || value.isEmpty()) {
                context.getCounter(ROW.FILTER).increment(1);
                return;
            }

            Params cmt = new ResultRDocMapper(value).map();
            if (cmt == null || cmt.getString("mid") == null)
                return;

            if (!cmt.containsKey("cmt_mid") && cmt.containsKey("pk")) {
                String pk = cmt.getString("pk");
                cmt.put("cmt_mid", pk.substring(3));
            }

            try {
                RhinoCmtWeiboEsDocMapper rhinoCmtWeiboEsDocMapper = RhinoCmtWeiboEsDocMapper.build(cmt.getString("mid"));
                Params esDoc = rhinoCmtWeiboEsDocMapper.map();

                if (esDoc != null) {
                    System.out.println(esDoc);
                    context.write(new Text(cmt.getString("pk")), esDoc);
                    context.getCounter(ROW.SHUFFLE).increment(1);
                } else {
                    context.getCounter(ROW.ERROR).increment(1);
                    System.err.println("[ErrHbDoc] " + cmt);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("[ERROR] " + cmt);
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
            return RhinoCmtWbESWriter.getInstance();
        }
    }
}
