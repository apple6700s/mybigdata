package com.datastory.banyan.asyncdata.kafka;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;

import java.util.Date;

/**
 * com.datastory.banyan.asyncdata.kafka.RhinoRealtimeConsumer
 * <p>
 * 高实时的Consumer
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class RhinoRealtimeConsumer extends RhinoAsyncDataConsumer {

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
        return 10;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_RT_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_RT_GRP);
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
        sparkConf.put("spark.executor.memory", "3000m");
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

        RhinoETLConfig.getInstance().set("default.suicide.time", "041500");
        new RhinoRealtimeConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
