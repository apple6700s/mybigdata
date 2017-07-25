package com.datastory.banyan.newsforum.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.filter.Filter;
import com.datastory.banyan.kafka.SparkYarnClusterKafkaConsumer;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumCmtWriter;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumPostWriter;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.newsforum.kafka.RhinoNewsForumYarnConsumer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class RhinoNewsForumYarnConsumer extends SparkYarnClusterKafkaConsumer {
    @Override
    public int getSparkCores() {
        return 60;
    }

    @Override
    public int getRepartitionNum() {
        return getSparkCores() * 2;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "500");
        sparkConf.put("spark.executor.memory", "3000m");
//        sparkConf.put("spark.streaming.concurrentJobs", "3");
        return sparkConf;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "" + 5 * 1024 * 1024);
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
        javaPairRDD
                .foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                final AtomicInteger docSize = new AtomicInteger(0);
                final ETLStatWrapper stat = new ETLStatWrapper();
                final NewsForumAnalyzer analyzer = NewsForumAnalyzer.getInstance();

                final PhoenixNewsForumCmtWriter cmtWriter = PhoenixNewsForumCmtWriter.getInstance();
                final PhoenixNewsForumPostWriter postWriter = PhoenixNewsForumPostWriter.getInstance();

                BlockProcessor blockProcessor = new BlockProcessor() {
                    @Override
                    public void _process(Object _p) {
                        JSONObject jsonObject = (JSONObject) _p;

                        List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();

                        if (list != null) {
                            Iterator<Params> iter = list.iterator();
                            while (iter.hasNext()) {
                                Params p = iter.next();
                                for (Filter filter : getFilters()) {
                                    if (filter.isFiltered(p)) {
                                        iter.remove();
                                        stat.filterStat().inc();
                                        break;
                                    }
                                }
                            }
                        }

                        if (CollectionUtil.isEmpty(list)) return;

                        // all_content
                        Params mainPost = null;
                        StringBuilder allContent = new StringBuilder();
                        for (Params p : list) {
                            LOG.info("pdate: " + p.getString("publish_date") + ", udate: " + p.getString("update_date"));
                            if (analyzer.isMainPost(p))
                                mainPost = p;
                            if (p.get("content") != null)
                                allContent.append(p.getString("content"));
                        }
                        if (mainPost != null)
                            mainPost.put("all_content", allContent.toString());

                        // analyz
                        for (int i = 0; i < list.size(); i++) {
                            final int id = i;
                            try {
                                Params p = list.get(id);
                                AccuStat s;
                                s = stat.analyzStat().tsafeBegin();
                                p = analyzer.analyz(p);
                                stat.analyzStat().tsafeEnd(s);

                                list.set(id, p);

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
                            System.out.println(DateUtils.getCurrentTimeStr() + "  batch size: " + batchSize + ", doc size: " + docSize);
                        }
                    }
                }.setPool(ThreadPoolWrapper.getInstance());

                // Consumer
                while (tuple2Iterator.hasNext()) {
                    String json = tuple2Iterator.next()._2();
                    if (StringUtil.isNullOrEmpty(json))
                        continue;
                    final JSONObject jsonObject = JSON.parseObject(json);
                    if (jsonObject == null || jsonObject.isEmpty())
                        continue;

                    blockProcessor.processAsync(jsonObject);
                }

                blockProcessor.cleanup();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new RhinoNewsForumYarnConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
