package com.datastory.banyan.wechat.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;
import com.datastory.banyan.wechat.es.WxMPESWriter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
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

    public void run() throws Exception {
        Scan scan = buildAllScan();
        scan.setFilter(new SingleColumnValueFilter("r".getBytes(), "open_id".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator()));

        Job job = buildJob(Tables.table(Tables.PH_WXMP_TBL), scan, WechatMPScanMapper.class, WechatMPFlushReducer.class);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new WechatMPFlushESMR().run();

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
            return WxMPESWriter.getInstance();
        }
    }
}
