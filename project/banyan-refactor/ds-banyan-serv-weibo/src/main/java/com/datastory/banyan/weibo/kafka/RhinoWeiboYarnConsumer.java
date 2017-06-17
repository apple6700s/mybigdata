package com.datastory.banyan.weibo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaReader;
import com.datastory.banyan.kafka.SparkYarnClusterKafkaConsumer;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.datastory.banyan.weibo.analyz.WbContentAnalyzer;
import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.datastory.banyan.weibo.doc.Status2HbParamsDocMapper;
import com.datastory.banyan.weibo.doc.User2HbParamsDocMapper;
import com.datastory.banyan.weibo.hbase.PhoenixWbContentWriter;
import com.datastory.banyan.weibo.hbase.PhoenixWbUserWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import weibo4j.model.Status;
import weibo4j.model.User;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.weibo.kafka.RhinoWeiboYarnConsumer
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class RhinoWeiboYarnConsumer extends SparkYarnClusterKafkaConsumer {
    @Override
    public int getSparkCores() {
        return 60;
    }

    @Override
    public int getRepartitionNum() {
        return 120;
//        return getSparkCores();
    }

    @Override
    public String getKafkaTopic() {
        return Tables.table(Tables.KFK_WB_TP);
    }

    @Override
    public String getKafkaGroup() {
        return Tables.table(Tables.KFK_WB_GRP);
    }


    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.streaming.receiver.maxRate", "2000");
        sparkConf.put("spark.executor.memory", "3500m");
        sparkConf.put("spark.streaming.concurrentJobs", "3");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                final AtomicInteger statusSize = new AtomicInteger(0);
                final AtomicInteger userSize = new AtomicInteger(0);
                final PhoenixWbContentWriter phoenixWbContentWriter = PhoenixWbContentWriter.getInstance();
                final PhoenixWbUserWriter phoenixWbUserWriter = PhoenixWbUserWriter.getInstance();
                ThreadPoolWrapper pool = ThreadPoolWrapper.getInstance(200);
                final CountUpLatch latch = new CountUpLatch();
                int size = 0;

                final ETLStatWrapper stat = new ETLStatWrapper();

                BlockProcessor blockProcessor = new BlockProcessor(500) {
                    @Override
                    public void _process(Object _p) {
                        try {
                            JSONObject jsonObject = (JSONObject) _p;
                            final Status status = GsonSerializer.deserialize(jsonObject.getString("json"), Status.class);
                            if (status.getUser() == null || StringUtils.isEmpty(status.getUser().getId())) {
                                stat.filterStat().inc();
                                return;
                            }
                            String updateDate = jsonObject.getString("update_date");

                            // split status & user
                            User statusUser = status.getUser();
                            // 当前微博系统显示转发的那一条就是源微博，用//等可以分隔出大致的转发关系，也就是说转发链被压缩到用户发的微博中。
                            Status srcStatus = status.getRetweetedStatus();
                            User srcStatusUser = srcStatus == null ? null : srcStatus.getUser();
                            LOG.info("udate: " + updateDate + ", pdate: " +
                                    DateUtils.getTimeStr(status.getCreatedAt()) + ", current: " + DateUtils.getCurrentTimeStr()
                            );

                            // doc mapping status & user
                            Params weibo = new Status2HbParamsDocMapper(status).map();
                            Params srcWeibo = new Status2HbParamsDocMapper(srcStatus).map();
                            Params user = new User2HbParamsDocMapper(statusUser).map();
                            Params srcUser = new User2HbParamsDocMapper(srcStatusUser).map();

                            if (weibo != null && user != null) {
                                user.put("last_tweet_date", weibo.getString("publish_date"));
                                if (user.get("uid") != null)
                                    weibo.put("uid", user.getString("uid"));
                            }

                            if (srcWeibo != null && srcUser != null) {
                                srcUser.put("last_tweet_date", srcWeibo.getString("publish_date"));
                                if (user.get("uid") != null)
                                    srcWeibo.put("uid", srcUser.getString("uid"));
                            }

                            // analyz
                            weibo = WbContentAnalyzer.getInstance().analyz(weibo);
                            srcWeibo = WbContentAnalyzer.getInstance().analyz(srcWeibo);
                            user = WbUserAnalyzer.getInstance().analyz(user);
                            srcUser = WbUserAnalyzer.getInstance().analyz(srcUser);

                            // write to hbase
                            try {
                                if (weibo != null) {
                                    phoenixWbContentWriter.batchWrite(weibo);
                                    statusSize.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }

                            try {
                                if (srcWeibo != null) {
                                    phoenixWbContentWriter.batchWrite(srcWeibo);
                                    statusSize.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }

                            try {
                                if (user != null) {
                                    phoenixWbUserWriter.batchWrite(user);
                                    userSize.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }

                            try {
                                if (srcUser != null) {
                                    phoenixWbUserWriter.batchWrite(srcUser);
                                    userSize.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }
                        } finally {
                            latch.countup();
                        }
                    }

                    @Override
                    public void cleanup() {
                        long size = processSize.get();
                        LOG.info("batch size: " + size + ", statusSize: " + statusSize + ", userSize: " + userSize);

                        if (size > 0) {
                            try {
                                phoenixWbContentWriter.flush();
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }
                            try {
                                phoenixWbUserWriter.flush();
                            } catch (Throwable e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    }
                }.setPool(pool).setup();

                // Consumer
                while (tuple2Iterator.hasNext()) {
                    try {
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;

                        JSONObject jsonObject = JSON.parseObject(jsonMsg);
                        size++;
                        blockProcessor.processAsync(jsonObject);
                    } catch (Throwable e) {
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

        new RhinoWeiboYarnConsumer().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
