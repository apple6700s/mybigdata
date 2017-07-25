package com.datastory.banyan.newsforum.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaDirectReader;
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
 * com.datastory.banyan.newsforum.kafka.RhinoNewsForumDirectConsumerBushu
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class RhinoNewsForumDirectConsumerBushu extends IKafkaDirectReader {
    public RhinoNewsForumDirectConsumerBushu() {
        RhinoETLConfig.getInstance().setBoolean("enable.es.writer", false);
    }

    @Override
    public int getSparkCores() {
        return 50;
    }

    @Override
    public int getRepartitionNum() {
        return 60;
    }

    @Override
    public int getStreamingMaxRate() {
        return 800;
    }

    public int getConditionOfExitJvm() {
        return 120;
    }

    public int getConditionOfMinRate() {
        return 100;
    }

    @Override
    public int getDurationSecond() {
        return 15;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.executor.memory", "4000m");
        sparkConf.put("spark.streaming.concurrentJobs", "2");
        sparkConf.put("spark.executor.cores", "2");

//        sparkConf.put("spark.streaming.backpressure.enabled", "false");
        sparkConf.put("spark.streaming.backpressure.pid.minRate", "400");
        return sparkConf;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "10000000");
        kafkaParam.put("auto.offset.reset", "smallest");
        return kafkaParam;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_LT_ALL_TP);
    }

    @Override
    public String getKafkaGroup() {
        return "consumer_group_news_all_v3_bushu";
    }

    public ProcessorRouter createRouter(final CountUpLatch latch) {
        LazyInitProcessorRouter router = new LazyInitProcessorRouter() {
            @Override
            public BlockProcessor route(Object msg) {
                JSONObject jsonObject = (JSONObject) msg;
                try {
                    String jobName = jsonObject.getString("jobName");
                    if (jobName.startsWith("DBA_qa_url")) {
                        return lazyInit(NFTrendCombineProcessor.class.getCanonicalName());
                    } else
                        // 未来需要爬虫组配合前缀进行更细粒度路由。
                        return lazyInit(NewsForumConsumeProcessor.class.getCanonicalName());
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                return null;
            }
        };

        router.registerCountUpLatchBlockProcessor(latch,
                NewsForumConsumeProcessor.class.getCanonicalName(),
                NFTrendCombineProcessor.class.getCanonicalName()
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
                            throw new Exception("[BANYAN] Cannot find processor, invalid routing msg : " + json);

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

        new RhinoNewsForumDirectConsumerBushu().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
        System.exit(0);
    }
}
