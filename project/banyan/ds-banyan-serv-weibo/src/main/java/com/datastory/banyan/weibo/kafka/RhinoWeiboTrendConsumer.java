package com.datastory.banyan.weibo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.weibo.doc.RhinoTrendDocMapper;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.weibo.kafka.RhinoWeiboTrendConsumer
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class RhinoWeiboTrendConsumer extends IKafkaReader {
    public RhinoWeiboTrendConsumer() {
        super();
        needReboot(true);
    }

    @Override
    public int getSparkCores() {
        return 2;
    }

    @Override
    public int getRepartitionNum() {
        return 0;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_WB_UP_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_WB_UP_GRP);
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.executor.memory", "1500m");
        sparkConf.put("spark.executor.cores", "2");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                int size = 0;

                BanyanRFieldPutter writer = new BanyanRFieldPutter(Tables.table(Tables.PH_TREND_TBL));
                ETLStatWrapper stat = new ETLStatWrapper();
                while (tuple2Iterator.hasNext()) {
                    try {
                        size++;
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;

                        JSONObject jsonObject = JSON.parseObject(jsonMsg);

                        Params trend = new RhinoTrendDocMapper(jsonObject).map();
                        if (trend == null) {
                            stat.filterStat().inc();
                            continue;
                        }

                        // write to hbase
                        HBaseUtils.writeTrend(writer, trend);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }

                if (size > 0) {
                    writer.flush();
                    LOG.info("batch size: " + size + ", stat: " + stat.filterStat());
                }
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        new RhinoWeiboTrendConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
