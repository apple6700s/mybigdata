package com.datastory.banyan.weibo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.weibo.kafka.processor.WeiboCmtConsumeProcessor;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.weibo.kafka.RhinoWeiboCommentConsumer
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class RhinoWeiboCommentBushuConsumer extends IKafkaReader {
    public RhinoWeiboCommentBushuConsumer() {
        super();
        needReboot(false);
    }

    @Override
    public int getDurationSecond() {
        return 10;
    }

    @Override
    public int getSparkCores() {
        return 100;
    }

    @Override
    public int getRepartitionNum() {
        return 100;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_WBCMT_TP);
    }

    @Override
    public String getKafkaGroup() {
        return "consumer.group.weibo.comment.refactor.bushu";
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.executor.memory", "4g");
        sparkConf.put("spark.executor.cores", "2");
        sparkConf.put("spark.streaming.backpressure.pid.minRate", "400");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                int size = 0;
                CountUpLatch latch = new CountUpLatch();
                WeiboCmtConsumeProcessor processor = new WeiboCmtConsumeProcessor(latch, false);
                while (tuple2Iterator.hasNext()) {
                    try {
                        size++;
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;

                        JSONObject jsonObject = JSON.parseObject(jsonMsg);
                        processor.processAsync(jsonObject);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }

                if (size > 0) {
                    latch.await(size);
                    processor.cleanup();
                    LOG.info("batch size = " + size);
                }
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new RhinoWeiboCommentBushuConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
