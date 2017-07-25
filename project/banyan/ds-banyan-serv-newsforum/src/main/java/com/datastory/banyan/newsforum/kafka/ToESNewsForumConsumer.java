package com.datastory.banyan.newsforum.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.ToESConsumer;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.kafka.ToESConsumerProcessorMap;
import com.datastory.banyan.newsforum.kafka.processor.NewsForumCmtToESProcessor;
import com.datastory.banyan.newsforum.kafka.processor.NewsForumPostToESProcessor;
import com.yeezhao.commons.util.Entity.StrParams;

import java.util.*;

/**
 * com.datastory.banyan.newsforum.kafka.ToESNewsForumConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class ToESNewsForumConsumer extends ToESConsumer {
    ToESConsumerProcessorMap processors = new ToESConsumerProcessorMap();

    @Override
    public int getSparkCores() {
        return 90;
    }

    @Override
    public int getRepartitionNum() {
        return getSparkCores() * 2;
    }

    public ToESNewsForumConsumer() {
        processors.add(new NewsForumPostToESProcessor());
        processors.add(new NewsForumCmtToESProcessor());
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "3000");
//        sparkConf.put("spark.executor.memory", "2500m");
        return sparkConf;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("auto.offset.reset", "largest");
        return kafkaParam;
    }

    @Override
    public Map<String, ToESConsumerProcessor> getProcessors() {
        return processors;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_PK_LT_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_PK_LT_GRP);
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new ToESNewsForumConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
