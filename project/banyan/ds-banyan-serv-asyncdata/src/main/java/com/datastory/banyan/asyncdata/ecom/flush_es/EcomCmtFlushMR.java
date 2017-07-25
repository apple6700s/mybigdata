package com.datastory.banyan.asyncdata.ecom.flush_es;

import com.datastory.banyan.asyncdata.ecom.doc.Hb2EsEcomCommentDocMapper;
import com.datastory.banyan.asyncdata.ecom.es.EcomCmtEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.spark.ScanFlushESMR;
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
 * com.datastory.banyan.asyncdata.ecom.flush_es.EcomCmtFlushMR
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class EcomCmtFlushMR extends ScanFlushESMR {
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
        String table = Tables.table(Tables.PH_ECOM_CMT_TBL);
        Job job = buildJob(table, scan, FlushMapper.class, FlushReducer.class);
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.memory.mb", "2048");
        job.setJobName(table + ": " + startUpdateDate + "-" + endUpdateDate);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        EcomCmtFlushMR mr = new EcomCmtFlushMR();
        if (args.length >= 1)
            mr.startUpdateDate = args[0];
        if (args.length >= 2) {
            mr.endUpdateDate = args[1];
        }

        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class FlushMapper extends ScanMapper {

        @Override
        public Params mapDoc(Params hbDoc) {
            return new Hb2EsEcomCommentDocMapper(hbDoc).map();
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
            if (hbDoc == null)
                return;
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

    public static class FlushReducer extends FlushESReducer {
        @Override
        public boolean isDebug() {
            return false;
        }

        @Override
        public ESWriterAPI getESWriter() {
            return EcomCmtEsWriter.getInstance();
        }
    }
}
