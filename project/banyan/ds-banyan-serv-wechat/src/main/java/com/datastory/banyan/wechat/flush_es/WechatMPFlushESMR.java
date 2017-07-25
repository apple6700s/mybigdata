package com.datastory.banyan.wechat.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxMPESWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.mapreduce.Job;

import java.util.Date;


/**
 * com.datastory.banyan.wechat.flush_es.WechatMPFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WechatMPFlushESMR extends ScanFlushESMR {
    private String startUpdateDate = null;
    private String endUpdateDate = null;

    @Override
    public String toString() {
        return startUpdateDate + " ~ " + endUpdateDate;
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
        String table = Tables.table(Tables.PH_WXMP_TBL);
        Job job = buildJob(table, scan, WechatMPScanMapper.class, WechatMPFlushReducer.class);
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.memory.mb", "2048");
        job.setJobName(table + ": " + startUpdateDate + "-" + endUpdateDate);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        WechatMPFlushESMR mr = new WechatMPFlushESMR();
        if (args.length >= 1)
            mr.startUpdateDate = args[0];
        if (args.length >= 2) {
            mr.endUpdateDate = args[1];
        }

        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WechatMPScanMapper extends ScanMapper {
        @Override
        public Params mapDoc(Params hbDoc) {
            return new WxMPHb2ESDocMapper(hbDoc).map();
        }
    }

    public static class WechatMPFlushReducer extends FlushESReducer {
        @Override
        public ESWriter getESWriter() {
            return WxMPESWriter.getInstance().setSyncMode();
        }
    }
}
