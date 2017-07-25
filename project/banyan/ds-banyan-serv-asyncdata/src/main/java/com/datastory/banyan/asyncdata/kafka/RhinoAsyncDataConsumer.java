package com.datastory.banyan.asyncdata.kafka;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.asyncdata.ecom.kafka.EcomCmtProcessor;
import com.datastory.banyan.asyncdata.ecom.kafka.EcomItemProcessor;
import com.datastory.banyan.asyncdata.video.kafka.VideoCmtProcessor;
import com.datastory.banyan.asyncdata.video.kafka.VideoPostProcessor;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.kafka.LazyInitProcessorRouter;
import com.datastory.banyan.kafka.ProcessorRouter;
import com.datastory.banyan.monitor.mon.AsyncTaskMonitor;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.banyan.wechat.kafka.processor.WechatConsumeProcessor;
import com.datastory.banyan.weibo.kafka.processor.WbAdvUserConsumeProcessor;
import com.datastory.banyan.weibo.kafka.processor.WeiboCmtConsumeProcessor;
import com.datastory.banyan.weibo.kafka.processor.WeiboConsumeProcessor;
import com.datastory.banyan.weibo.kafka.processor.WeiboTrendConsumeProcessor;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.RuntimeUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * com.datastory.banyan.asyncdata.kafka.RhinoAsyncDataConsumer
 * <p>
 * 风险：ecom和video这样的comment和post分开，有可能comment的post不存在或漏数？
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class RhinoAsyncDataConsumer extends IKafkaReader {
    protected ProcessorRouter createRouter(final CountUpLatch latch) {
        LazyInitProcessorRouter router = new LazyInitProcessorRouter() {
            @Override
            public BlockProcessor route(Object msg) {
                JSONObject jsonObject = (JSONObject) msg;
                try {
                    // 1 for ecom, 4 for video
                    Integer catId = null;
                    boolean hasParent = false;

                    if (jsonObject.containsKey("cat_id")) {
                        catId = jsonObject.getInteger("cat_id");
                        hasParent = jsonObject.containsKey("unique_parent_id");
                    } else {
                        if (jsonObject.containsKey("info")) {
                            JSONObject info = jsonObject.getJSONObject("info");
                            if (info.containsKey("cat_id")) {
                                catId = info.getInteger("cat_id");
                                hasParent = jsonObject.containsKey("unique_parent_id");
                            }
                        }
                    }

                    if (catId != null) {
                        if (catId == 1) {
                            if (!hasParent) {
                                return lazyInit(EcomItemProcessor.class.getCanonicalName());
                            } else {
                                return lazyInit(EcomCmtProcessor.class.getCanonicalName());
                            }
                        } else if (catId == 4) {
                            if (!hasParent) {
                                return lazyInit(VideoPostProcessor.class.getCanonicalName());
                            } else {
                                return lazyInit(VideoCmtProcessor.class.getCanonicalName());
                            }
                        } else if (catId == 9) {
                            if (jsonObject.containsKey("parent_id")) {
                                return lazyInit(WeiboCmtConsumeProcessor.class.getCanonicalName());
                            } else if (jsonObject.containsKey("attitudes_count")) {
                                return lazyInit(WeiboTrendConsumeProcessor.class.getCanonicalName());
                            } else if (jsonObject.containsKey("type")) {
                                return lazyInit(WbAdvUserConsumeProcessor.class.getCanonicalName());
                            } else {
                                return lazyInit(WeiboConsumeProcessor.class.getCanonicalName());
                            }
                        } else if (catId == 10) {
                            return lazyInit(WechatConsumeProcessor.class.getCanonicalName());
                        }
                    } else if (jsonObject.containsKey("type") && jsonObject.containsKey("uid")) {
                        return lazyInit(WbAdvUserConsumeProcessor.class.getCanonicalName());
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                return null;
            }
        };

        // register processors
        router.registerCountUpLatchBlockProcessor(latch,
                // 电商商品
                EcomItemProcessor.class.getCanonicalName(),
                // 电商评论
                EcomCmtProcessor.class.getCanonicalName(),
                // 视频主贴
                VideoPostProcessor.class.getCanonicalName(),
                // 视频评论
                VideoCmtProcessor.class.getCanonicalName(),
                // 微博内容
                WeiboConsumeProcessor.class.getCanonicalName(),
                // 微博高级用户
                WbAdvUserConsumeProcessor.class.getCanonicalName(),
                // 微博评论
                WeiboCmtConsumeProcessor.class.getCanonicalName(),
                // 微博趋势
                WeiboTrendConsumeProcessor.class.getCanonicalName(),
                // 微信内容
                WechatConsumeProcessor.class.getCanonicalName()
        );

        return router;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        final String klass = this.getClass().getSimpleName();
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> iter) throws Exception {
                int size = 0;
                CountUpLatch latch = new CountUpLatch();
                FreqDist<String> progresses = new FreqDist<>();

                ProcessorRouter router = createRouter(latch);
                while (iter.hasNext()) {
                    try {
                        String msg = iter.next()._2();
                        JSONObject jsonObject = JSONObject.parseObject(msg);
                        if (jsonObject == null)
                            throw new Exception("[BANYAN] JsonObject is null");
                        BlockProcessor blockProcessor = router.route(jsonObject);
                        if (blockProcessor == null)
                            throw new Exception("[BANYAN] Cannot find processor, invalid routing msg : " + msg);

                        size++;
                        blockProcessor.processAsync(jsonObject);
                        boolean needProgress = AsyncTaskMonitor.needRecordProgress(jsonObject);
                        if (needProgress) {
                            String jobName = AsyncTaskMonitor.getJobName(jsonObject);
                            if (jobName != null) {
                                progresses.inc(jobName);
                            } else {
                                LOG.error("No taskId : " + msg);
                            }
                        }

                        String prefix = blockProcessor.getClass().getSimpleName().replace("Processor", "");
                        ErrorUtil.infoConsumingProgress(LOG, prefix, jsonObject, null);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }

                latch.await(size);
                router.cleanup();
                if (size > 0) {
                    LOG.info("[AsyncData] batch size=" + size);
                    AsyncTaskMonitor.getInstance().inc(progresses);
                } else {
                    Random random = new Random();
                    int r = random.nextInt(10);
                    if (r == 0) {
                        LOG.info("[AsyncData] batch size=" + size);
                    }
                }
            }
        });
    }

    @Override
    public int getSparkCores() {
        return 50;
    }

    @Override
    public int getDurationSecond() {
        return 10;
    }

    @Override
    public int getStreamingMaxRate() {
        return 800;
    }

    @Override
    public int getConditionOfExitJvm() {
        return 100;
    }

    @Override
    public int getConditionOfMinRate() {
        return 120;
    }

    @Override
    public int getKafkaPartitions() {
        return 5;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_ADATA_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_ADATA_GRP);
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "10000000");
        kafkaParam.put("auto.offset.reset", "smallest");
        return kafkaParam;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.executor.memory", "2000m");
        sparkConf.put("spark.streaming.concurrentJobs", "2");
        sparkConf.put("spark.executor.cores", "2");

        sparkConf.put("spark.streaming.backpressure.pid.minRate", "400");
        return sparkConf;
    }


    /**
     * main
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        RhinoETLConfig.getInstance().set("default.suicide.time", "230000");
        new RhinoAsyncDataConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
