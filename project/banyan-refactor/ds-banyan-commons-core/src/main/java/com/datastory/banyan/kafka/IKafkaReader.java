package com.datastory.banyan.kafka;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.filter.Filter;
import com.datastory.banyan.monitor.utils.StreamingConsumerDaemon;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.commons.kafka.consumer.KafkaDStreamConsumer;
import com.datastory.commons.spark.client.streaming.ScheduledRateController;
import com.datastory.commons.spark.client.streaming.StreamingYarnAmReboot;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;


/**
 * com.datastory.banyan.kafka.IKafkaReader
 *
 * @author lhfcws
 * @since 16/11/23
 */

public abstract class IKafkaReader extends KafkaDStreamConsumer implements StreamingYarnAmReboot {
    private static final long serialVersionUID = 20121212l;
    protected boolean needReboot = true;
    protected List<Filter> filters = new LinkedList<>();

    public IKafkaReader() {
        super(RhinoETLConfig.getInstance());
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public IKafkaReader addFilter(Filter filter) {
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
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.default.parallelism", getSparkCores() * 2 + "");
        sparkConf.put("spark.executor.memory", "2000M");
        sparkConf.put("spark.executor.cores", "2");
        sparkConf.put("spark.ui.showConsoleProgress", "false");
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.streaming.backpressure.pid.minRate", "100");
        sparkConf.put("spark.streaming.concurrentJobs", "2");
        sparkConf.put("spark.streaming.stopGracefullyOnShutdown", "false"); // use shutdown hook
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

    public JavaStreamingContext createJavaStreamingContext(SparkConf sparkConf) {
        int duration = this.getDurationSecond();
        int numThreads = this.getKafkaPartitions();
        StrParams kafkaParams = this.customizedKafkaParams();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds((long) duration));
        jssc.remember(DFT_REMEMBER);
        this.configSparkContext(jssc.sc());

        HashMap<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put(this.getKafkaTopic(), numThreads);
        JavaPairReceiverInputDStream stream = KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicThreadMap, StorageLevel.MEMORY_AND_DISK());
        LOG.info("kafka inputStream init success");
        LOG.info("start " + this.getAppName());

        // 添加流量控制，不行就kill掉自己。由守护进程重新拉起。
        if (jssc.sc().getConf().getBoolean("spark.streaming.backpressure.enabled", false)) {
            LOG.info("Add backpressure support.");
            ScheduledRateController rateController = new ScheduledRateController(getDurationSecond(), jssc);
            rateController.setConditionOfExitJvm(this, getConditionOfExitJvm());
            rateController.setAggressiveRate(getStreamingMaxRate() / 3);
            rateController.setMaxCowardTimes(3);
            rateController.setConditionOfMinRate(stream.receiverInputDStream(), getConditionOfMinRate());
            try {
                rateController.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            this.processStream(stream);
        } catch (Exception var9) {
            var9.printStackTrace();
            LOG.error(var9.getMessage(), var9);
        }

        return jssc;
    }

    @Override
    public void init(final JavaStreamingContext jssc) {
        initCancelTimer(jssc);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                ErrorUtil.print("[SHUTDOWN HOOK]", "invoke jssc.stop(true, true)");
                SparkUtil.stopStreamingContext(jssc, getDurationSecond(), getAppName());
                ErrorUtil.print("[SHUTDOWN HOOK]", "done jssc.stop(true, true)");
            }
        }));
    }

    public void needReboot(boolean var1) {
        needReboot = var1;
    }

}
