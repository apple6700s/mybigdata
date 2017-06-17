package com.datastory.banyan.kafka;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.async.Function;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * com.datastory.banyan.kafka.ToESConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */

//public abstract class ToESConsumer extends SparkYarnClusterKafkaConsumer {
public abstract class ToESConsumer extends IKafkaReader {
    public abstract Map<String, ToESConsumerProcessor> getProcessors();

    public int getThreadPoolSize() {
        return 100;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "1000");
        sparkConf.put("spark.streaming.concurrentJobs", "3");
        sparkConf.put("spark.executor.memory", "2500m");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                ThreadPoolWrapper pool = ThreadPoolWrapper.getInstance(getThreadPoolSize());
                final CountUpLatch latch = new CountUpLatch();
                int size = 0;

                BlockProcessor blockProcessor = new BlockProcessor() {
                    @Override
                    public void _process(Object _p) {
                        try {
                            String json = (String) _p;
                            if (processSize.get() % 10 == 1)
                                LOG.info("[SAMPLE] " + json);
                            ToESKafkaProtocol protocol = ToESKafkaProtocol.fromJson(json);
                            ToESConsumerProcessor toESConsumerProcessor = getProcessors().get(protocol.getTable());
                            try {
                                toESConsumerProcessor.process(protocol);
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                            }
                        } finally {
                            latch.countup();
                        }
                    }

                    @Override
                    public void cleanup() {
                        System.out.println("Batch size: " + processSize.get());
                        for (final ToESConsumerProcessor processor : getProcessors().values()) {
                            Async.async(AsyncPool.RUN_POOL, new Function() {
                                @Override
                                public AsyncRet call() throws Exception {
                                    processor.flush();
                                    return null;
                                }
                            });
                        }
                    }
                }.setPool(pool);

                // Consumer
                while (tuple2Iterator.hasNext()) {
                    String json = tuple2Iterator.next()._2();
                    if (StringUtils.isEmpty(json))
                        continue;
                    size++;
                    blockProcessor.processAsync(json);
                }

                latch.await(size);
                blockProcessor.cleanup();
            }
        });
    }
}
