package com.datastory.banyan.weibo.abel;

import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * com.datastory.banyan.weibo.flush_es.WeiboContentFlushESMR
 * <p/>
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WeiboContentFlushESMR extends ScanFlushESMR {
    private String startPostDate = null;
    private String endPostDate = null;
    private String outputPath = "/tmp/abel.chan/imagedt/weibo";

    @Override
    public String toString() {
        return startPostDate + " ~ " + endPostDate;
    }

    public int getReducerNum() {
        return 50;
    }

    public void run() throws Exception {
        Scan scan = buildAllScan();
//        scan.addColumn("r".getBytes(),)
        FilterList filterList = new FilterList();


        if (startPostDate != null) {
            SingleColumnValueFilter startFilter = new SingleColumnValueFilter("r".getBytes(), "publish_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startPostDate.getBytes());

            filterList.addFilter(startFilter);
        }
        if (endPostDate != null) {
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter("r".getBytes(), "publish_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, endPostDate.getBytes());
            filterList.addFilter(endFilter);
        }

        if (startPostDate != null || endPostDate != null)
            scan.setFilter(filterList);

        System.out.println("[CONDITION] " + startPostDate + " ~ " + endPostDate);
        System.out.println("[SCAN] " + scan);

//        scan.setStartRow("0001090925883987".getBytes()).setStopRow("0002011103092332050234".getBytes());
        String table = "DS_BANYAN_WEIBO_CONTENT_V1";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
        outputPath = outputPath + "-" + sdf.format(new Date());
        Job job = buildJob(table, scan, WeiboContentScanMapper.class, WeiboContentFlushReducer.class, outputPath);
//        job.setJobName(table + ": " + startPostDate + "-" + endPostDate);
        job.setJobName(table);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        WeiboContentFlushESMR mr = new WeiboContentFlushESMR();
        if (args.length >= 1)
            mr.startPostDate = args[0];
        if (args.length >= 2) {
            mr.endPostDate = args[1];
        }
        mr.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public static class WeiboContentScanMapper extends ScanMapper {


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
            try {

                if (!StringUtil.isNullOrEmpty(hbDoc.getString("mid"))) {
                    context.getCounter(ROW.ROWS).increment(1);
                }

//                if (!StringUtil.isNullOrEmpty(hbDoc.getString("pic_urls"))) {
//                    String mid = hbDoc.getString("mid");
//                    String uid = hbDoc.getString("uid");
//                    String content = StringUtil.isNullOrEmpty(hbDoc.getString("content")) ? "" : hbDoc.getString("content");
//                    String publishDate = hbDoc.getString("publish_date");
//                    String picRrls = hbDoc.getString("pic_urls");
//                    Integer repostCnt = null;
//                    try {
//                        repostCnt = hbDoc.getInt("reposts_cnt");
//                    } catch (Exception e) {
//                    }
//                    Integer commentsCnt = null;
//                    try {
//                        commentsCnt = hbDoc.getInt("comments_cnt");
//                    } catch (Exception e) {
//                    }
//                    Integer attitudesCnt = null;
//                    try {
//                        attitudesCnt = hbDoc.getInt("attitudes_cnt");
//                    } catch (Exception e) {
//                    }
//                    Params result = new Params();
//                    result.put("uid", uid);
//                    result.put("mid", mid);
//                    result.put("publish_date", publishDate);
//                    result.put("pic_urls", picRrls);
//                    result.put("content", content);
//                    if (repostCnt != null) {
//                        result.put("reposts_cnt", repostCnt);
//                    }
//                    if (commentsCnt != null) {
//                        result.put("comments_cnt", commentsCnt);
//                    }
//                    if (attitudesCnt != null) {
//                        result.put("attitudes_cnt", attitudesCnt);
//                    }
//                    context.write(new Text(mid), result);
//                    context.getCounter(ROW.SHUFFLE).increment(1);
//                }
            } catch (Exception e) {
                context.getCounter(ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + hbDoc);
            }

//            if (!StringUtil.isNullOrEmpty(hbDoc.getString("content"))) {
//                String selfContent = SelfContentExtractor.extract(hbDoc.getString("content"));
//                if (!StringUtil.isNullOrEmpty(selfContent)) {
//                    hbDoc.put("self_content", selfContent);
//                    hbDoc.put("self_content_len", selfContent.length());
//                }
//            }
//
//            Params esDoc = mapDoc(hbDoc);
//
//            if (esDoc != null) {
//                context.write(new Text(routing(pk)), esDoc);
//                context.getCounter(ROW.SHUFFLE).increment(1);
//            } else {
//                context.getCounter(ROW.ERROR).increment(1);
//                System.err.println("[ErrHbDoc] " + hbDoc);
//            }
        }
    }

    public static class WeiboContentFlushReducer extends FlushESReducer {
        @Override
        public boolean isDebug() {
            return false;
        }

    }
}
