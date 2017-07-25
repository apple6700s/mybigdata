package com.datastory.banyan.weibo.abel;

import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * com.datastory.banyan.weibo.flush_es.WeiboUserFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WeiboUserNegBiFanCntFlushESMR extends ScanFlushESMR {

    private String outputPath = "/tmp/abel.chan/imagedt/user";

    public int getReducerNum() {
        return 2;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
        scan.addColumn("r".getBytes(), "bi_follow_cnt".getBytes());

//        scan.addColumn("r".getBytes(),)
        FilterList filterList = new FilterList();

        SingleColumnValueFilter startFilter = new SingleColumnValueFilter("r".getBytes(), "bi_follow_cnt".getBytes(), CompareFilter.CompareOp.LESS, "0".getBytes());

        filterList.addFilter(startFilter);

        scan.setFilter(filterList);
//        scan.setStartRow("19e".getBytes()).setStopRow("19f".getBytes());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
        outputPath = outputPath + "-" + sdf.format(new Date());
        Job job = buildJob(Tables.table(Tables.PH_WBUSER_TBL), scan, WeiboUserNegBiFanCntScanMapper.class, WeiboUserNegBiFanCntFlushReducer.class, outputPath);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {

        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new WeiboUserNegBiFanCntFlushESMR().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WeiboUserNegBiFanCntScanMapper extends ScanMapper {

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (result == null || result.isEmpty())
                return;
            String pk = new String(result.getRow());
            if (pk.length() < 4)
                return;
            String value = new String(result.getValue("r".getBytes(), "bi_follow_cnt".getBytes()));
            try {
                Params params = new Params();
                params.put("count", 1);
                context.write(new Text(value), params);
            } catch (Exception e) {
                context.getCounter(ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + pk);
            }


        }
    }

    public static class WeiboUserNegBiFanCntFlushReducer extends FlushESReducer {

        @Override
        protected void reduce(Text key, Iterable<Params> values, final Context context) throws IOException, InterruptedException {

            long size = 0;
            for (Params p : values) {
                size++;
            }
            context.write(key, new Text(String.valueOf(size)));
            context.getCounter(ROW.WRITE).increment(1);
        }

        public boolean isDebug() {
            return false;
        }
    }
}
