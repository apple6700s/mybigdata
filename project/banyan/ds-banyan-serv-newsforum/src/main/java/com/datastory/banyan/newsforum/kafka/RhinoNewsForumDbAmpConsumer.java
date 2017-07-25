package com.datastory.banyan.newsforum.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.kafka.LazyInitProcessorRouter;
import com.datastory.banyan.kafka.ProcessorRouter;
import com.datastory.banyan.newsforum.kafka.processor.NFTrendCombineProcessor;
import com.datastory.banyan.newsforum.kafka.processor.NewsForumConsumeProcessor;
import com.datastory.banyan.utils.CountUpLatch;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.newsforum.kafka.RhinoNewsForumDbAmpConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class RhinoNewsForumDbAmpConsumer extends IKafkaReader {
    public RhinoNewsForumDbAmpConsumer() {
        this.needReboot = false;
    }

    @Override
    public int getSparkCores() {
        return 2;
    }

    @Override
    public int getRepartitionNum() {
        return 10;
    }

    @Override
    public int getStreamingMaxRate() {
        return 600;
    }

    public int getConditionOfExitJvm() {
        return 100;
    }

    public int getConditionOfMinRate() {
        return 120;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.executor.memory", "4000m");
//        sparkConf.put("spark.streaming.concurrentJobs", "3");
        return sparkConf;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "10000000");
        kafkaParam.put("auto.offset.reset", "largest");
        return kafkaParam;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_LT_ALL_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_LT_ALL_GRP) + ".dbamp.0711";
    }

    public ProcessorRouter createRouter(final CountUpLatch latch) {
        LazyInitProcessorRouter router = new LazyInitProcessorRouter() {
            @Override
            public BlockProcessor route(Object msg) {
                JSONObject jsonObject = (JSONObject) msg;
                try {
                    String jobName = jsonObject.getString("jobName");
                    if (jobName.startsWith("DBA_qa_url")) {
                        LOG.info(jsonObject);
                        return lazyInit(NFTrendCombineProcessor.class);
                    } else
                        return null;
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                return null;
            }
        };

        router.registerCountUpLatchBlockProcessor(latch,
                NewsForumConsumeProcessor.class,
                NFTrendCombineProcessor.class
        );

        return router;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                int size = 0;
                CountUpLatch latch = new CountUpLatch();
                ProcessorRouter router = createRouter(latch);

                while (tuple2Iterator.hasNext()) {
                    try {
                        String json = tuple2Iterator.next()._2();
                        if (StringUtil.isNullOrEmpty(json))
                            continue;
                        JSONObject jsonObject = JSON.parseObject(json);
                        if (jsonObject == null || jsonObject.isEmpty())
                            continue;

                        BlockProcessor processor = router.route(jsonObject);
                        if (processor == null)
                            continue;

                        size++;
                        processor.processAsync(jsonObject);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }

                latch.await(size);
                router.cleanup();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        new RhinoNewsForumDbAmpConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
