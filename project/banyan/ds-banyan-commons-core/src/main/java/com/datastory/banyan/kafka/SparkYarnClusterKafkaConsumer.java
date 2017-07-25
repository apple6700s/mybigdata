package com.datastory.banyan.kafka;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.filter.Filter;
import com.datastory.commons.kafka.consumer.KafkaDStreamConsumer;
import com.datastory.commons.spark.client.SparkYarnClusterSubmit;
import com.datastory.commons.spark.client.streaming.ScheduledRateController;
import com.datastory.commons.spark.client.streaming.StreamingContextStarter;
import com.datastory.commons.spark.client.streaming.StreamingRunner;
import com.yeezhao.commons.util.Entity.StrParams;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * com.datastory.banyan.kafka.SparkYarnClusterKafkaConsumer
 * <p>
 * KafkaStreamConsumer on spark yarn-cluster , 继承这个类的实现的main函数不是用来启动的！！
 * 要用 SparkStreamingYarnLauncher 来启动
 *
 * @author lhfcws
 * @since 2016/12/21
 */
@Deprecated
public abstract class SparkYarnClusterKafkaConsumer extends KafkaDStreamConsumer {
    protected List<Filter> filters = new LinkedList<>();

    public SparkYarnClusterKafkaConsumer() {
        super(RhinoETLConfig.getInstance());
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public SparkYarnClusterKafkaConsumer addFilter(Filter filter) {
        this.filters.add(filter);
        return this;
    }

    @Override
    public void init() {

    }

    @Override
    public String getZkConn() {
        return RhinoETLConfig.getInstance().getKafkaZkQuorum();
    }

    public String getKafkaBrokersList() {
        String kafkaBrokersList = RhinoETLConfig.getInstance().get(RhinoETLConsts.KAFKA_BROKER_LIST);
        System.out.println("[KafkaBrokersList] " + kafkaBrokersList);
        return kafkaBrokersList;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "1000");
        sparkConf.put("spark.executor.memory", "2500m");
        return sparkConf;
    }

    @Override
    public int getDurationSecond() {
        return 10;
    }

    public int getMaxSmoothBatchNum() {
        return 300;
    }

    public int getMaxReceivedBatchNum() {
        return 500;
    }

    @Override
    public int getRepartitionNum() {
        return 120;
    }

    @Override
    public int getSparkCores() {
        return 40;
    }

    public SparkConf createSparkConf() {
        SparkConf sparkConf = SparkYarnClusterSubmit.getLocalOrNewSparkConf();
        for (Tuple2<String, String> tuple: sparkConf.getAll()) {
            System.out.println(tuple._1() + " = " + tuple._2() + "\n");
        }
        return sparkConf;
    }

    public JavaStreamingContext createJavaStreamingContext(SparkConf sparkConf) {
        int durationSec = getDurationSecond();
        final StreamingContextStarter starter = new StreamingContextStarter(sparkConf, durationSec);
        starter.setRunner(new StreamingRunner() {
            @Override
            public void run(JavaStreamingContext jssc, StreamingContextStarter streamingContextStarter) throws Exception {
                int numThreads = getKafkaPartitions();
                StrParams kafkaParams = customizedKafkaParams();
                Map<String, Integer> topicThreadMap = new HashMap<>();
                topicThreadMap.put(getKafkaTopic(), numThreads);

                // create kafka input stream
                JavaPairReceiverInputDStream<String, String> stream;
                stream = KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class
                        , StringDecoder.class
                        , kafkaParams, topicThreadMap, StorageLevel.MEMORY_AND_DISK_SER());

                LOG.info("[BANYAN] kafka inputStream init success");
                LOG.info("[BANYAN] start " + getAppName());

                ScheduledRateController rateController =
                        new ScheduledRateController(starter.getDurationSec(), jssc);
                // 当 received unprocessed batch 数超过个时，将receiver rate调整到minRate
                rateController.setConditionOfMinRate(stream.receiverInputDStream(), getMaxSmoothBatchNum());
                // 当 received unprocessed batch 数超过个时，将平滑关闭JavaStreamingContext
                // 实际业务，可以调用ScheduledRateController#caculateMaxCapacityOfBatches
                // 来根据jvm最大内存来获取最大的waiting batches
//                rateController.setConditionOfExitJvm(starter, getMaxReceivedBatchNum());
                rateController.start();

                try {
                    processStream(stream);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error(e.getMessage(), e);
                }
            }
        });

        try {
            starter.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
