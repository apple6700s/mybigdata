package com.datastory.banyan.weibo.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.ToESConsumer;
import com.datastory.banyan.kafka.ToESConsumerProcessorMap;
import com.datastory.banyan.weibo.kafka.processor.WeiboContentToESProcessor;
import com.datastory.banyan.weibo.kafka.processor.WeiboUserToESProcessor;
import com.yeezhao.commons.util.Entity.StrParams;

import java.util.Date;

/**
 * com.datastory.banyan.weibo.kafka.ToESWeiboConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class ToESWeiboConsumer extends ToESConsumer {
    ToESConsumerProcessorMap processors = new ToESConsumerProcessorMap();

    @Override
    public int getSparkCores() {
        return 100;
    }

    @Override
    public int getRepartitionNum() {
        return 33;
    }

    public ToESWeiboConsumer() {
        this.processors.add(new WeiboContentToESProcessor());
        this.processors.add(new WeiboUserToESProcessor());
    }

    @Override
    public ToESConsumerProcessorMap getProcessors() {
        return processors;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_PK_WB_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_PK_WB_GRP);
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.concurrentJobs", "3");
        return sparkConf;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new ToESWeiboConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
