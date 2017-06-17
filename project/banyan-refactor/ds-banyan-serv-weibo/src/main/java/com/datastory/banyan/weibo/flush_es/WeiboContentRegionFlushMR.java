package com.datastory.banyan.weibo.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.weibo.es.WbCntESWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;

/**
 * com.datastory.banyan.weibo.flush_es.WeiboContentRegionFlushMR
 *
 * @author lhfcws
 * @since 2016/12/21
 */
public class WeiboContentRegionFlushMR extends WeiboContentFlushESMR {
    protected String startKey;
    protected String endKey;

    public int getReducerNum() {
        return 30;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
        if (!StringUtils.isEmpty(startKey))
            scan.setStartRow(startKey.getBytes());
        if (!StringUtils.isEmpty(endKey))
            scan.setStopRow(endKey.getBytes());

        System.out.println("[CONDITION] " + startKey + " ~ " + endKey);
        System.out.println("[SCAN] " + scan);
//        scan.setStartRow("0001090925883987".getBytes()).setStopRow("0002011103092332050234".getBytes());
        String table = Tables.table(Tables.PH_WBCNT_TBL);
        Job job = buildJob(table, scan, WeiboContentScanMapper.class, WeiboRegionContentFlushReducer.class);
        job.setJobName(table + ": " + startKey + "-" + endKey);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        WeiboContentRegionFlushMR mr = new WeiboContentRegionFlushMR();

        if (args.length != 2) {
            System.err.println("Invalid args, exited. args length = " + args.length);
            return;
        }

        mr.startKey = args[0].trim();
        mr.endKey = args[1].trim();

        if ("*".equals(mr.startKey) || "'*'".equals(mr.startKey))
            mr.startKey = null;
        if ("*".equals(mr.endKey) || "'*'".equals(mr.endKey))
            mr.endKey = null;

        mr.run();

        System.out.println("[PROGRAM] Program exited.");
    }

    public static class WeiboRegionContentFlushReducer extends FlushESReducer {
        @Override
        public boolean isDebug() {
            return false;
        }

        @Override
        public ESWriterAPI getESWriter() {
            return WbCntESWriter.getFlushInstance(Tables.ES_WB_IDX, 5000);
        }
    }
}
