package com.datastory.banyan.weibo.flush_es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HbaseScanner;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.doc.WbCmtEsDocMapper;
import com.datastory.banyan.weibo.doc.WbCmtWeiboEsDocMapper;
import com.datastory.banyan.weibo.es.CmtESWriter;
import com.datastory.banyan.weibo.es.CmtWbESWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.quartz.SchedulerException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * com.datastory.banyan.weibo.flush_es.CommentFlusher
 *
 * @author lhfcws
 * @since 2017/6/2
 */
public class CommentFlusher extends HbaseScanner.SparkHBaseScanner {


    public CommentFlusher() {
        setTable(Tables.table(Tables.PH_WBCMT_TBL));
    }

    public void setTimeRange(String start, String end) {
        Scan scan = HBaseUtils.buildScan();
        FilterList filterList = new FilterList();
        if (start != null)
            filterList.addFilter(new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, start.getBytes()));
        if (end != null)
            filterList.addFilter(new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, end.getBytes()));

        if (!filterList.getFilters().isEmpty())
            scan.setFilter(filterList);
        setScan(scan);
        setAppName(this.getClass().getSimpleName() + " : " + start + " ~ " + end);
    }

    public static StrParams getConfigs() {
        return new StrParams(BanyanTypeUtil.strArr2strMap(new String[]{
                "spark.executor.memory", "3500m",
                "spark.cores.max", "50"
        }));
    }

    public void run() {
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sparkScan(SparkRunner.class, 50, getConfigs());
        new SparkRunner().run(this.getJsc(), hbaseRDD);
    }

    public static class SparkRunner implements Serializable {
        public void run(JavaSparkContext jsc, JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD) {
            final Accumulator<Integer> totalAcc = jsc.accumulator(0);
            final Accumulator<Integer> weiboAcc = jsc.accumulator(0);

            JavaRDD<String> rdd = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    totalAcc.add(1);
                    Params cmt = new ResultRDocMapper(v1._2()).map();
                    String mid = cmt.getString("mid");

                    Params esCmt = new WbCmtEsDocMapper(cmt).map();

                    CmtESWriter.getInstance().setSyncMode();
                    CmtESWriter.getInstance().write(esCmt);

                    return mid;
                }
            });

            rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER());

            rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> stringIterator) throws Exception {
                    CmtESWriter.getInstance().flush();
                }
            });

            rdd.distinct(24).foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iter) throws Exception {
                    CmtWbESWriter.getInstance().setSyncMode();
                    HBaseReader reader = new HBaseReader(Tables.table(Tables.PH_WBCNT_TBL));
                    while (iter.hasNext()) {
                        weiboAcc.add(1);
                        String mid = iter.next();
                        List<Params> res = reader.batchRead(BanyanTypeUtil.wbcontentPK(mid));
                        if (BanyanTypeUtil.valid(res)) {
                            flush(res);
                        }
                    }

                    List<Params> res = reader.flush();
                    if (BanyanTypeUtil.valid(res)) {
                        flush(res);
                    }
                    CmtWbESWriter.getInstance().flush();
                }

                public void flush(List<Params> res) {
                    for (Params p : res) {
                        if (p != null) {
                            Params esDoc = new WbCmtWeiboEsDocMapper(p).map();
                            CmtWbESWriter.getInstance().write(esDoc);
                        }
                    }
                }
            })
            ;

            System.out.println("\n=====================");
            System.out.println("comment total: " + totalAcc);
            System.out.println("weibo total: " + weiboAcc);
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        CommentFlusher runner = new CommentFlusher();
        String startUpdateDate = null, endUpdateDate = null;
        if (args.length >= 1)
            startUpdateDate = args[0];
        if (args.length >= 2) {
            endUpdateDate = args[1];
        }

        runner.setTimeRange(startUpdateDate, endUpdateDate);
        runner.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    /**
     * 每天零点起，跑昨天update的数据
     */
    public static class CommentFlushTimer {
        public static void quartz() throws SchedulerException {
            QuartzExecutor executor = new QuartzExecutor() {
                @Override
                public void execute() {
                    Date date = new Date();
                    date.setSeconds(0);
                    date.setMinutes(0);
                    date.setHours(0);

                    String start = DateUtils.getTimeStr(date);

                    CommentFlusher commentFlusher = new CommentFlusher();
                    commentFlusher.setTimeRange(start, null);
                    commentFlusher.run();
                }
            };

            QuartzJobUtils.createQuartzJob("0 0 0 * * ?", "CommentFlush", executor);
        }
    }
}
