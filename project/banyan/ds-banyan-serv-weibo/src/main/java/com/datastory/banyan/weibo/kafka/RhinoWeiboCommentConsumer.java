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

public class RhinoWeiboCommentConsumer extends IKafkaReader {
    public RhinoWeiboCommentConsumer() {
        super();
        needReboot(true);
//        try {
//            CommentFlusher.CommentFlushTimer.quartz();
//        } catch (SchedulerException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public int getDurationSecond() {
        return 15;
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
        return Tables.table(Tables.KFK_WBCMT_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_WBCMT_GRP);
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.executor.memory", "2g");
        sparkConf.put("spark.executor.cores", "2");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                int size = 0;
                CountUpLatch latch = new CountUpLatch();
                WeiboCmtConsumeProcessor processor = new WeiboCmtConsumeProcessor(latch);
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

        new RhinoWeiboCommentConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
        System.exit(0);
    }
}
