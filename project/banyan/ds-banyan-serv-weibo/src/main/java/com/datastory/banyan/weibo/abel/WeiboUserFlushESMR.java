package com.datastory.banyan.weibo.abel;

import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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

public class WeiboUserFlushESMR extends ScanFlushESMR {

    private String outputPath = "/tmp/abel.chan/imagedt/user";

    public int getReducerNum() {
        return 120;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
//        scan.setStartRow("19e".getBytes()).setStopRow("19f".getBytes());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
        outputPath = outputPath + "-" + sdf.format(new Date());
        Job job = buildJob(Tables.table(Tables.PH_WBUSER_TBL), scan, WeiboUserScanMapper.class, WeiboUserFlushReducer.class, outputPath);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new WeiboUserFlushESMR().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WeiboUserScanMapper extends ScanMapper {

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (result == null || result.isEmpty())
                return;
            String pk = new String(result.getRow());
            if (pk.length() < 4)
                return;
            Params hbDoc = new ResultRDocMapper(result).map();
            try {
                String vtype = hbDoc.getString("vtype");
                String uid = hbDoc.getString("uid");

                Params newParams = new Params();
                newParams.put("uid", uid);
                newParams.put("vtype", vtype);
                context.write(new Text(uid), newParams);
            } catch (Exception e) {
                context.getCounter(ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + hbDoc);
            }


        }
    }

    public static class WeiboUserFlushReducer extends FlushESReducer {
        public boolean isDebug() {
            return false;
        }
    }
}
