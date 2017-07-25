package com.datastory.banyan.weibo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaDirectReader;
import com.datastory.banyan.monitor.mon.AsyncTaskMonitor;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.datastory.banyan.weibo.kafka.processor.WeiboConsumeProcessor;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.FreqDist;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.weibo.kafka.RhinoWeiboDirectConsumerBushu
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class RhinoWeiboDirectConsumerBushu extends IKafkaDirectReader {
    public RhinoWeiboDirectConsumerBushu() {
//        RhinoETLConfig.getInstance().setBoolean("enable.es.writer", false);
    }

    @Override
    public int getSparkCores() {
        return 50;
    }

    @Override
    public int getRepartitionNum() {
        return 70;
    }

    @Override
    public String getKafkaTopic() {
        return "topic_rhino_weibo_bushu";
    }

    @Override
    public String getKafkaGroup() {
        return "banyan_weibo_bushu";
    }

    @Override
    public int getDurationSecond() {
        return 15;
    }

    @Override
    public int getStreamingMaxRate() {
        return 800;
    }

    public int getConditionOfExitJvm() {
        return 120;
    }

    public int getConditionOfMinRate() {
        return 100;
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
        sparkConf.put("spark.executor.memory", "4000m");
        sparkConf.put("spark.streaming.concurrentJobs", "2");
        sparkConf.put("spark.executor.cores", "2");

        sparkConf.put("spark.streaming.backpressure.pid.minRate", "400");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                // Consumer
                CountUpLatch latch = new CountUpLatch();
                FreqDist<String> progresses = new FreqDist<>();

                int size = 0;
                BlockProcessor blockProcessor = null;
                while (tuple2Iterator.hasNext()) {
                    if (blockProcessor == null) {
                        blockProcessor = initBlockProcessor(latch);
                    }

                    try {
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;

                        JSONObject jsonObject = JSON.parseObject(jsonMsg);
                        boolean needProgress = AsyncTaskMonitor.needRecordProgress(jsonObject);
                        if (needProgress) {
                            progresses.inc(AsyncTaskMonitor.getJobName(jsonObject));
                        }

                        size++;
                        blockProcessor.processAsync(jsonObject);
                    } catch (Throwable e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                latch.await(size);
                if (blockProcessor != null)
                    blockProcessor.cleanup();
                if (size > 0) {
                    AsyncTaskMonitor.getInstance().inc(progresses);
                }
            }
        });
    }

    protected static BlockProcessor initBlockProcessor(final CountUpLatch latch) {
        return new WeiboConsumeProcessor(latch)
                .setPool(ThreadPoolWrapper.getInstance(200)).setup();
    }

    static class Test {
        public static void main(String[] args) throws Exception {
            String jsonMsg = "{\"sourceCrawlerId\":\"1759\",\"publish_date\":\"20170328060708\",\"msgDepth\":\"1\",\"site_id\":\"101993\",\"jobName\":\"weibo_batch_20170329000228_394_17\",\"lang\":\"\",\"pageNum\":\"50\",\"cat_id\":\"9\",\"CONTROL_AMOUNT\":\"39\",\"time_zone\":\"Asia/Shanghai\",\"end_date\":\"20170329000000\",\"uids\":\"1402314312,1402599940,1402394544,1402245304,1402582644,1402434397,1402295681,1402400261,1402315543,1402246547,1402567654,1402543043,1402418937,1402336565,1402477151,1402297502,1402372727,1402470365,1402340315,1402249090\",\"_html_\":\"避免过长，省略...\",\"json\":\"{\\\"user\\\":{\\\"id\\\":\\\"1402400261\\\",\\\"screenName\\\":\\\"爱可可-爱生活\\\",\\\"name\\\":\\\"爱可可-爱生活\\\",\\\"province\\\":11,\\\"city\\\":5,\\\"location\\\":\\\"北京 朝阳区\\\",\\\"description\\\":\\\"北邮PRIS模式识别实验室陈老师\\\",\\\"url\\\":\\\"http://blog.fly51fly.com\\\",\\\"profileImageUrl\\\":\\\"http://tva2.sinaimg.cn/crop.10.34.646.646.50/5396ee05jw1ena6co8qiwj20sg0izjxd.jpg\\\",\\\"userDomain\\\":\\\"fly51fly\\\",\\\"gender\\\":\\\"m\\\",\\\"followersCount\\\":150650,\\\"friendsCount\\\":855,\\\"statusesCount\\\":31618,\\\"favouritesCount\\\":1529,\\\"createdAt\\\":\\\"Jan 30, 2010 8:06:34 AM\\\",\\\"following\\\":false,\\\"verified\\\":true,\\\"verifiedType\\\":0,\\\"allowAllActMsg\\\":false,\\\"allowAllComment\\\":true,\\\"followMe\\\":false,\\\"avatarLarge\\\":\\\"http://tva2.sinaimg.cn/crop.10.34.646.646.180/5396ee05jw1ena6co8qiwj20sg0izjxd.jpg\\\",\\\"onlineStatus\\\":0,\\\"biFollowersCount\\\":716,\\\"lang\\\":\\\"zh-cn\\\",\\\"verifiedReason\\\":\\\"知名互联网博主 微博签约自媒体\\\",\\\"weihao\\\":\\\"\\\",\\\"statusId\\\":\\\"\\\"},\\\"createdAt\\\":\\\"Mar 28, 2017 6:07:08 AM\\\",\\\"id\\\":\\\"4090155091934103\\\",\\\"mid\\\":\\\"4090155091934103\\\",\\\"idstr\\\":4090155091934103,\\\"text\\\":\\\"【面向设计师的机器学习】《Machine Learning for Designers》by Patrick Hebron http://t.cn/R6a7JC0 \u200B\\\",\\\"source\\\":{\\\"url\\\":\\\"http://app.weibo.com/t/feed/8gLGL\\\",\\\"relationShip\\\":\\\"nofollow\\\",\\\"name\\\":\\\"Mac客户端\\\"},\\\"favorited\\\":false,\\\"truncated\\\":false,\\\"inReplyToStatusId\\\":-1,\\\"inReplyToUserId\\\":-1,\\\"inReplyToScreenName\\\":\\\"\\\",\\\"thumbnailPic\\\":\\\"http://wx2.sinaimg.cn/thumbnail/5396ee05ly1fe24uirnxqj20w01bwqrp.jpg\\\",\\\"bmiddlePic\\\":\\\"http://wx2.sinaimg.cn/bmiddle/5396ee05ly1fe24uirnxqj20w01bwqrp.jpg\\\",\\\"originalPic\\\":\\\"http://wx2.sinaimg.cn/large/5396ee05ly1fe24uirnxqj20w01bwqrp.jpg\\\",\\\"geo\\\":\\\"null\\\",\\\"latitude\\\":-1.0,\\\"longitude\\\":-1.0,\\\"repostsCount\\\":30,\\\"commentsCount\\\":8,\\\"attitudesCount\\\":13,\\\"annotations\\\":\\\"\\\",\\\"mlevel\\\":0,\\\"feature\\\":1,\\\"picUrls\\\":[\\\"http://wx2.sinaimg.cn/thumbnail/5396ee05ly1fe24uirnxqj20w01bwqrp.jpg\\\",\\\"http://wx1.sinaimg.cn/thumbnail/5396ee05ly1fe24v9lm0pj20sk0i2n0g.jpg\\\",\\\"http://wx3.sinaimg.cn/thumbnail/5396ee05ly1fe24vhw3aqj20si10sqv5.jpg\\\",\\\"http://wx1.sinaimg.cn/thumbnail/5396ee05ly1fe24vq5rpqj20sc0f8gy9.jpg\\\",\\\"http://wx3.sinaimg.cn/thumbnail/5396ee05ly1fe24vv37s0j20s80nq11r.jpg\\\",\\\"http://wx2.sinaimg.cn/thumbnail/5396ee05ly1fe24w2pwmhj20ss0motbw.jpg\\\",\\\"http://wx4.sinaimg.cn/thumbnail/5396ee05ly1fe24w70oboj20sk0jw76z.jpg\\\",\\\"http://wx2.sinaimg.cn/thumbnail/5396ee05ly1fe24wdlq9wj20rw0lswy4.jpg\\\",\\\"http://wx1.sinaimg.cn/thumbnail/5396ee05ly1fe24wqwgadj20sm0kuwi2.jpg\\\"],\\\"visible\\\":{\\\"type\\\":0,\\\"list_id\\\":0}}\",\"update_date\":\"20170329000520\",\"msgType\":\"1\",\"mid\":\"4090155091934103\",\"start_date\":\"20170328000000\"}";
            BlockProcessor blockProcessor = initBlockProcessor(null);
            blockProcessor.process(JSON.parseObject(jsonMsg));
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        new RhinoWeiboDirectConsumerBushu().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
        System.exit(0);
    }
}
