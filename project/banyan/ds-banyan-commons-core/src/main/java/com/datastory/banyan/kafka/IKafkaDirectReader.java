package com.datastory.banyan.kafka;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.filter.Filter;
import com.datastory.banyan.monitor.utils.StreamingConsumerDaemon;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ShutdownHookManger;
import com.datastory.commons.kafka.consumer.KafkaDirectStreamConsumer;
import com.datastory.commons.spark.client.streaming.ScheduledRateController;
import com.datastory.commons.spark.client.streaming.StreamingYarnAmReboot;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.DsKafkaUtils;
import org.apache.spark.streaming.scheduler.RateController;

import java.io.IOException;
import java.util.*;


/**
 * com.datastory.banyan.kafka.IKafkaReader
 *
 * @author lhfcws
 * @since 16/11/23
 */

public abstract class IKafkaDirectReader extends KafkaDirectStreamConsumer implements StreamingYarnAmReboot {
    private static final long serialVersionUID = 20121212l;
    protected boolean needReboot = true;
    protected List<Filter> filters = new LinkedList<>();

    public IKafkaDirectReader() {
        super(RhinoETLConfig.getInstance());
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public IKafkaDirectReader addFilter(Filter filter) {
        this.filters.add(filter);
        return this;
    }

    @Override
    public void init() {
        String name = this.getAppName();
        // daemon
        try {
            StreamingConsumerDaemon.daemon(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // enable checkpoint
        setEnableCheckpoint(true);
        setCheckpointHDFS("/tmp/banyan/checkpoint/" + name);
    }

    protected void initCancelTimer(final JavaStreamingContext jssc) {
        if (needReboot) {
            String dt = RhinoETLConfig.getInstance().get("default.suicide.time");
            if (StringUtil.isNullOrEmpty(dt))
                return;

            dt = "20170101" + dt;

            try {
                Date date = DateUtils.parse(dt, DateUtils.DFT_TIMEFORMAT);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                Calendar now = Calendar.getInstance();

                calendar.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.get(Calendar.DAY_OF_MONTH));
                if (now.after(calendar))
                    calendar.add(Calendar.DAY_OF_MONTH, 1);
                date = calendar.getTime();
                LOG.info("Cancel time : " + date);

                Timer cancelTimer = new Timer("cancelTimer");
                cancelTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if (needReboot) {
                            LOG.info("Stop JavaStreamingContext at " + DateUtils.getCurrentPrettyTimeStr());
                            jssc.stop(true, true);
                            jssc.close();
                            System.exit(0);
                        }
                    }
                }, date);
            } catch (Exception ignore) {

            }
        }
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
    public int getDurationSecond() {
        return 15;
    }

    @Override
    public int getRepartitionNum() {
        return 60;
    }

    @Override
    public int getSparkCores() {
        return 20;
    }

    public int getStreamingMaxRate() {
        return 800;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParams = super.customizedKafkaParams();
        // remove 2 deprecated properties
        kafkaParams.remove("session.timeout.ms");
        kafkaParams.remove("heartbeat.interval.ms");
        return kafkaParams;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        int partitionRate = getStreamingMaxRate() / getKafkaPartitions();
        if (partitionRate <= 0) partitionRate = getStreamingMaxRate();

        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.default.parallelism", getSparkCores() * 2 + "");
        sparkConf.put("spark.executor.memory", "2000M");
        sparkConf.put("spark.executor.cores", "2");
        sparkConf.put("spark.rpc.askTimeout", "800");
        sparkConf.put("spark.network.timeout", "800");
        sparkConf.put("spark.ui.showConsoleProgress", "false");
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.streaming.kafka.maxRatePerPartition", partitionRate + "");
        sparkConf.put("spark.streaming.backpressure.pid.minRate", "100");
        sparkConf.put("spark.streaming.concurrentJobs", "2");
//        sparkConf.put("spark.streaming.stopGracefullyOnShutdown", "false"); // use shutdown hook
        sparkConf.put("spark.streaming.gracefulStopTimeout", "86400s");
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.spark.BanyanKryoRegister");
        String sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.executor.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.put("spark.executor.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC -Dlog4j.configuration=file:/opt/package/spark/conf/log4j.simple.properties");
        sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.driver.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.put("spark.driver.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC");
        return sparkConf;
    }

    public int getConditionOfExitJvm() {
        return 80;
    }

    public int getConditionOfMinRate() {
        return 50;
    }

    protected static Duration DFT_REMEMBER = new Duration(2 * 3600 * 1000 + 60 * 1000);

    @Override
    public JavaStreamingContext createJavaStreamingContext(final SparkConf sparkConf) {
        final int duration = getDurationSecond();
        int numThreads = getKafkaPartitions();
        final StrParams kafkaParams = customizedKafkaParams();

        if (isEnableCheckpoint()) {
            try {
                buildCheckpointDir(checkpointHDFS);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        final StreamingYarnAmReboot _this = this;
        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
                // Create a factory object that can create a and setup a new JavaStreamingContext
                loadOffsets();
                initZKOffsetStorage();
                final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));
                if (isEnableCheckpoint()) {
                    jssc.checkpoint(checkpointHDFS);                       // set checkpoint directory
                }

                Map<String, Integer> topicThreadMap = new HashMap<>();
                topicThreadMap.put(getKafkaTopic(), getKafkaPartitions());

                JavaPairInputDStream<String, String> stream;

                Set<String> topics = new HashSet<>();
                topics.add(getKafkaTopic());

                stream = DsKafkaUtils.createDirectStream(jssc,
                        kafkaParams, topics, offsets,
                        String.class, String.class, StringDecoder.class, StringDecoder.class);
                if (isEnableCheckpoint())
                    stream.checkpoint(Durations.seconds(duration));

                LOG.info("kafka inputDirectStream init success");
                LOG.info("start " + getAppName());
                configSparkContext(jssc.sc());

                // 添加流量控制，不行就kill掉自己。由守护进程重新拉起。
                if (jssc.sc().getConf().getBoolean("spark.streaming.backpressure.enabled", false)) {
                    LOG.info("Add backpressure support.");
                    ScheduledRateController rateController = new ScheduledRateController(getDurationSecond(), jssc);
                    rateController.setConditionOfExitJvm(_this, getConditionOfExitJvm());
                    rateController.setAggressiveRate(getStreamingMaxRate() / 3);
                    rateController.setMaxCowardTimes(3);
                    RateController rc = (RateController) stream.inputDStream().rateController().get();
                    rateController.setConditionOfMinRate(rc, getConditionOfMinRate());
//                    rateController.setHookLatestRate(new Function<Long, Long>() {
//                        @Nullable
//                        @Override
//                        public Long apply(@Nullable final Long latestRate) {
//                            Async.async(AsyncPool.MONITOR_POOL, new com.datastory.banyan.async.Function() {
//                                @Override
//                                public AsyncRet call() throws Exception {
//                                    if (RhinoETLConfig.getInstance().getBoolean("enable.falcon.monitor", true)) {
//                                        String key = getKafkaTopic() + " - " + getKafkaGroup();
//                                        FalconKeyRegister.register(key);
//                                        FalconMetricMonitor.getInstance().inc(key, latestRate);
//                                    }
//                                    return null;
//                                }
//                            });
//                            return latestRate;
//                        }
//                    });
                    try {
                        rateController.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    ShutdownHookManger.addShutdownHook("jssc.stop(true, true)", new Runnable() {
                        public void run() {
                            SparkUtil.stopStreamingContext(jssc, getDurationSecond(), getAppName());

                        }
                    });
                }

                try {
                    processStream(stream);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error(e.getMessage(), e);
                }

                return jssc;
            }
        };

        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointHDFS, contextFactory);
        return jssc;
    }

    @Override
    protected void addShutdownHook(JavaStreamingContext jssc) {

    }

    @Override
    public void init(final JavaStreamingContext jssc) {
        initCancelTimer(jssc);

    }

    public void needReboot(boolean var1) {
        needReboot = var1;
    }

}
