package com.datastory.banyan.wechat.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.SparkYarnClusterKafkaConsumer;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.datastory.banyan.wechat.analyz.WechatEssayAnalyzer;
import com.datastory.banyan.wechat.doc.RhinoWechatContentDocMapper;
import com.datastory.banyan.wechat.doc.RhinoWechatMPDocMapper;
import com.datastory.banyan.wechat.hbase.PhoenixWxCntWriter;
import com.datastory.banyan.wechat.hbase.PhoenixWxMPWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.wechat.kafka.RhinoWechatYarnConsumer
 *
 * @author lhfcws
 * @since 16/12/6
 */
@Deprecated
public class RhinoWechatYarnConsumer extends SparkYarnClusterKafkaConsumer {
    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_WX_CNT_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_WX_CNT_GRP);
    }

    @Override
    public int getRepartitionNum() {
        return 29;
    }

    @Override
    public int getSparkCores() {
        return 30;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.executor.memory", "2500m");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                final PhoenixWxCntWriter cntWriter = PhoenixWxCntWriter.getInstance();
                final PhoenixWxMPWriter mpWriter = PhoenixWxMPWriter.getInstance();
                final CountUpLatch latch = new CountUpLatch();
                int size = 0;

                BlockProcessor blockProcessor = new BlockProcessor(300) {
                    @Override
                    public void _process(Object _p) {
                        try {
                            JSONObject jsonObject = (JSONObject) _p;
                            Params wechat = new RhinoWechatContentDocMapper(jsonObject).map();
                            Params mp = new RhinoWechatMPDocMapper(jsonObject).map();

                            if (wechat != null) {
                                LOG.info("[WECHAT] udate: " + wechat.getString("update_date") +
                                        ", pdate: " + wechat.getString("publish_date") +
                                        ", pk: " + wechat.getString("pk")
                                );
                                wechat = WechatEssayAnalyzer.getInstance().analyz(wechat);

                                cntWriter.batchWrite(wechat);
                            }
                            if (mp != null) {
                                LOG.info("[MP] udate: " + mp.getString("update_date") +
                                        ", pk: " + wechat.getString("pk")
                                );
                                mpWriter.batchWrite(mp);
                            }
                        } finally {
                            latch.countup();
                        }
                    }

                    @Override
                    public void cleanup() {
                        LOG.info("Batch size => " + processSize.get());
                        if (processSize.get() > 0) {
                            try {
                                cntWriter.flush();
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                            }
                            try {
                                mpWriter.flush();
                            } catch (Exception e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    }
                }.setPool(ThreadPoolWrapper.getInstance()).setup();

                // Consumer
                while (tuple2Iterator.hasNext()) {
                    try {
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        final String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;
                        JSONObject jsonObject = JSON.parseObject(jsonMsg);

                        size ++;
                        blockProcessor.processAsync(jsonObject);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                latch.await(size);
                blockProcessor.cleanup();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new RhinoWechatYarnConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
