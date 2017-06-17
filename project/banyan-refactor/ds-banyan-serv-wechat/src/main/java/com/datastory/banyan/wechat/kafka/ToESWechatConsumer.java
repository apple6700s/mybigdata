package com.datastory.banyan.wechat.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.ToESConsumer;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.kafka.ToESConsumerProcessorMap;
import com.datastory.banyan.wechat.kafka.processor.WxContentToESProcessor;
import com.datastory.banyan.wechat.kafka.processor.WxMPToESProcessor;
import com.yeezhao.commons.util.Entity.StrParams;

import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.wechat.kafka.ToESWechatConsumer
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class ToESWechatConsumer extends ToESConsumer {
    ToESConsumerProcessorMap toESConsumerProcessorMap = new ToESConsumerProcessorMap();

    public ToESWechatConsumer() {
        toESConsumerProcessorMap.add(new WxContentToESProcessor());
        toESConsumerProcessorMap.add(new WxMPToESProcessor());
    }

    @Override
    public int getSparkCores() {
        return 30;
    }

    @Override
    public int getRepartitionNum() {
        return 29;
    }

    @Override
    public Map<String, ToESConsumerProcessor> getProcessors() {
        return toESConsumerProcessorMap;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_PK_WX_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_PK_WX_GRP);
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "3000");
//        sparkConf.put("spark.executor.memory", "2400m");
        return sparkConf;
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new ToESWechatConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
