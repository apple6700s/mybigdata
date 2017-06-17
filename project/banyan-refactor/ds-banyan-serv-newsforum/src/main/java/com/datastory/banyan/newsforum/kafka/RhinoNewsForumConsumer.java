package com.datastory.banyan.newsforum.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumCmtWriter;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumPostWriter;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.newsforum.kafka.RhinoNewsForumConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class RhinoNewsForumConsumer extends IKafkaReader {
    @Override
    public int getSparkCores() {
        return 80;
    }

    @Override
    public int getRepartitionNum() {
        return 150;
    }

    @Override
    public int getStreamingMaxRate() {
        return 800;
    }

    public int getConditionOfExitJvm() {
        return 80;
    }

    public int getConditionOfMinRate() {
        return 120;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", getStreamingMaxRate() + "");
        sparkConf.put("spark.executor.memory", "4000m");
//        sparkConf.put("spark.streaming.concurrentJobs", "3");
        return sparkConf;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "10000000");
        return kafkaParam;
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_LT_ALL_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_LT_ALL_GRP);
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        final int expectBatch = getStreamingMaxRate() * getDurationSecond();
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                final AtomicInteger docSize = new AtomicInteger(0);
                final ETLStatWrapper stat = new ETLStatWrapper();
                final NewsForumAnalyzer analyzer = NewsForumAnalyzer.getInstance();

                final PhoenixNewsForumCmtWriter cmtWriter = PhoenixNewsForumCmtWriter.getInstance(3000);
                final PhoenixNewsForumPostWriter postWriter = PhoenixNewsForumPostWriter.getInstance(3000);

                final CountUpLatch latch = new CountUpLatch();
                int size = 0;

                BlockProcessor blockProcessor = new BlockProcessor(300) {
                    @Override
                    public void _process(Object _p) {
                        try {
                            JSONObject jsonObject = (JSONObject) _p;

                            List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();

                            if (CollectionUtil.isEmpty(list)) return;

                            // all_content
                            Params mainPost = list.get(0);
                            if (!analyzer.isMainPost(mainPost))
                                mainPost = null;

                            if (mainPost != null) {
                                StringBuilder allContent = new StringBuilder();
                                if (mainPost.getString("content") != null)
                                    allContent.append(mainPost.getString("content"));

                                for (int i = 1; i < list.size(); i++) {
                                    Params p = list.get(i);
                                    if (p.get("content") != null)
                                        allContent.append(p.getString("content"));
                                }
                                mainPost.put("all_content", allContent.toString());
                            }

                            // analyz
                            for (int i = 0; i < list.size(); i++) {
                                try {
                                    Params p = list.get(i);
                                    LOG.info("pdate: " + p.getString("publish_date") + ", udate: " + p.getString("update_date"));
                                    AccuStat s;
                                    s = stat.analyzStat().tsafeBegin();
                                    p = analyzer.analyz(p);
                                    stat.analyzStat().tsafeEnd(s);

                                    list.set(i, p);

                                    s = stat.hbaseStat().tsafeBegin();
                                    if (analyzer.isMainPost(p))
                                        postWriter.batchWrite(p);
                                    else
                                        cmtWriter.batchWrite(p);
                                    stat.hbaseStat().tsafeEnd(s);
                                } catch (Throwable e) {
                                    LOG.error(e.getMessage(), e);
                                } finally {
                                }
                            }
                            docSize.addAndGet(list.size());
                        } catch (Exception e) {
                            LOG.error("[PROCESS] " + e.getMessage() + " , " + _p);
                        } finally {
                            latch.countup();
                        }
                    }

                    @Override
                    public void cleanup() {
                        if (processSize.get() > 0) {
                            try {
                                cmtWriter.flush();
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }

                            try {
                                postWriter.flush();
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }
                            LOG.info("batch size: " + batchSize + ", doc size: " + docSize);
                        } else {
                            ESWriterAPI esWriterAPI = cmtWriter.getEsWriter();
                            if (esWriterAPI instanceof ESWriter) {
                                ESWriter esWriter = (ESWriter) esWriterAPI;
                                esWriter.closeIfIdle();
                            }

                            esWriterAPI = postWriter.getEsWriter();
                            if (esWriterAPI instanceof ESWriter) {
                                ESWriter esWriter = (ESWriter) esWriterAPI;
                                esWriter.closeIfIdle();
                            }
                        }
                    }
                }.setPool(ThreadPoolWrapper.getInstance()).setup();

                // Consumer
                while (tuple2Iterator.hasNext()) {
                    String json = tuple2Iterator.next()._2();
                    if (StringUtil.isNullOrEmpty(json))
                        continue;
                    final JSONObject jsonObject = JSON.parseObject(json);
                    if (jsonObject == null || jsonObject.isEmpty())
                        continue;

                    size++;
                    blockProcessor.processAsync(jsonObject);
                }
                latch.await(size);
                blockProcessor.cleanup();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        new RhinoNewsForumConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
