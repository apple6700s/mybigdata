package com.datastory.banyan.wechat.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.kafka.IKafkaDirectReader;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;
import com.datastory.banyan.wechat.kafka.processor.WechatConsumeProcessor;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * com.datastory.banyan.wechat.kafka.RhinoWechatDirectConsumer
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class RhinoWechatDirectConsumer extends IKafkaDirectReader {
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
        return 80;
//        return getSparkCores();
    }

    @Override
    public int getSparkCores() {
        return 70;
    }

    @Override
    public int getStreamingMaxRate() {
        return 600;
    }

    public int getConditionOfExitJvm() {
        return 120;
    }

    public int getConditionOfMinRate() {
        return 80;
    }

    @Override
    public StrParams customizedKafkaParams() {
        StrParams kafkaParam = super.customizedKafkaParams();
        kafkaParam.put("fetch.message.max.bytes", "" + (20 * 1024 * 1024));
        kafkaParam.put("auto.offset.reset", "smallest");
        return kafkaParam;
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.executor.memory", "3000m");
        sparkConf.put("spark.executor.cores", "2");
        return sparkConf;
    }

    @Override
    protected void consume(JavaPairRDD<String, String> javaPairRDD) {
        final int expectBatch = getStreamingMaxRate() * getDurationSecond();
        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                final CountUpLatch latch = new CountUpLatch();

                BlockProcessor blockProcessor = null;
                // Consumer
                int size = 0;
                while (tuple2Iterator.hasNext()) {
                    if (blockProcessor == null) {
                        blockProcessor = new WechatConsumeProcessor(latch)
                                .setPool(ThreadPoolWrapper.getInstance()).setup();
                    }

                    try {
                        Tuple2<String, String> tuple = tuple2Iterator.next();
                        final String jsonMsg = tuple._2();
                        if (StringUtil.isNullOrEmpty(jsonMsg))
                            continue;
                        JSONObject jsonObject = JSON.parseObject(jsonMsg);

                        size++;
                        blockProcessor.processAsync(jsonObject);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                latch.await(size);
                if (blockProcessor != null)
                    blockProcessor.cleanup();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " System started. " + new Date());

        new RhinoWechatDirectConsumer().run();
//        String jsonMsg = "{\"src_url\":\"http://www.zgsydw.com/kaoshitiku/mryl/\",\"jobName\":\"weixin_job_20170112000055_605_3\",\"cat_id\":\"8\",\"author\":\"山西事业考试指南\",\"end_date\":\"20170112000000\",\"article_id\":\"3421fba128da0550cb4e1e7e3c233cce\",\"title\":\"公共基础知识每日一练（51期）\",\"_html_\":\"避免过长，省略...\",\"is_main_post\":\"1\",\"site\":\"搜狗微信\",\"sourceCrawlerId\":\"70\",\"clear_url\":\"http://mp.weixin.qq.com/s?__biz=MjM5NDYyMjA2NA==&mid=2652753948&idx=4&sn=29277a88ce6f25e94a5b2c9068cf15ad\",\"publish_date\":\"20170111144838\",\"keyword\":\"oIWsFt-hMfISDiF2lmS4-jwcf5B8\",\"msgDepth\":\"3\",\"site_id\":\"17\",\"other_data\":\"{\\\"sn\\\":\\\"29277a88ce6f25e94a5b2c9068cf15ad\\\",\\\"desc\\\":\\\"中公教育始创于1999年，业务领域涵盖公职类考试、企事业单位招聘考试、职业资格认证考试、考研、就业技能培训等全方位职业就业培训项目，是全国性综合职业教育企业。在全国31个省市建立470家直营分校和旗舰学习中心，拥有近6000人专职教师团队。\\\",\\\"idx\\\":\\\"4\\\",\\\"wxid\\\":\\\"shxsydw\\\",\\\"biz\\\":\\\"MjM5NDYyMjA2NA\\\\u003d\\\\u003d\\\"}\",\"url\":\"http://mp.weixin.qq.com/s?__biz=MjM5NDYyMjA2NA==&mid=2652753948&idx=4&sn=29277a88ce6f25e94a5b2c9068cf15ad&chksm=bd6d8f038a1a06158243b64e4f27208401bdc31286c104ecdbdfc20cf35729ee06e384772caf\",\"biz\":\"MjM5NDYyMjA2NA==\",\"is_original\":\"0\",\"content\":\"17省直十五轮线上模考大赛第四轮模考（回复关键字“第四轮”即可参加）：1月14日早上9点开始，15日晚上22点结束，16日晚19点中公十九课堂免费答案解析，随后发布电子版答案~~ 1.述职报告的写作要求是(　　)。 A.标题要清楚，内容要全面，语言要庄重，个性要鲜明，详略要搭档 B.标题要清楚，内容要客观，重点要突出，个性要鲜明，语言要庄重 C.标准要清楚，内容要客观，重点要突出，个性要鲜明，语言要朴实 D.标题要清楚，内容要客观，个性要鲜明，详略要得当，语言要朴实 2.我国最早的人类是(　　)。 A.北京人 B.蓝田人 C.元谋人 D.南方古猿 3.科举考试的殿试始于(　　)。 A.唐太宗 B.唐高宗 C.武则天 D.唐玄宗 4.第三世界在国际政治舞台崛起，并成为决定国际事务的一支主要力量的体现是(　　)。 A.1955年亚非会议的召开 B.20世纪60年代初不结盟运动的形成 C.20世纪60年代七十七国集团的建立 D.东欧剧变苏联解体 5.《金粉世家》的作者是(　　)。 A.徐枕亚 B.张恨水 C.周瘦鹃 D.包天笑 6.事业单位开展专业业务活动及其辅助活动取得的收入叫做(　　)。 A.业务活动利润 B.经营收入 C.事业收入 D.商业收入 参考答案 1.【答案】B。中公教育解析：述职报告的写作要求：标准要清楚、内容要客观、重点要突出、个性要鲜明、语言要庄重。故本题答案选B。 2.【答案】C。中公教育解析：国境内发现的远古居民主要有元谋人、北京人、山顶洞人、蓝田人。元谋人生活在距今约一百七十万年，是我国境内已知的最早人类;北京人生活距今约七十万年至二十万年;山顶洞人生活距今约三万年。蓝田人是中国的直立人化石生活距今110万年前到115万年前。故本题答案选C。 3.【答案】B。中公教育解析：显庆四年(659年)，唐高宗亲自在大殿上开科取士，由皇帝亲自监考选拔人才，是科举史上第一次“殿试”，此次殿试规模不大。故本题答案选B。 4.【答案】B。中公教育解析：不结盟运动形成以后，它们在反对帝国主义、殖民主义，促进亚非拉各国民族解放运动的深入发展;在反对霸权主义、国际强权政治和集团政治，维护第三世界国家的独立、主权和平等地位;在反对超级大国侵略和战争政策，保卫世界和平和各国安全;在改革旧的国际经济关系，建立国际经济新秩序等方面，作出了不懈的努力。故本题答案选B。 5.【答案】B。中公教育解析：张恨水(1897年5月18日—1967年2月15日)，原名心远，笔名恨水，取南唐李煜词《乌夜啼》“自是人生长恨水长东”之意。张恨水是著名章回小说家，也是“鸳鸯蝴蝶派”代表作家。作品情节曲折复杂，结构布局严谨完整，将中国传统的章回体小说与西洋小说的新技法融为一体。以《春明外史》、《金粉世家》、《啼笑因缘》、《八十一梦》四部长篇小说为代表作。故本题答案选B。 6.【答案】C。中公教育解析：事业收入，即事业单位开展专业业务活动及其辅助活动取得的收入，比如学校的学费收入、医院的医疗收入等。故本题答案选C。 往期推荐 公共基础知识每日一练（46期） 公共基础知识每日一练（47期） 公共基础知识每日一练（48期） 公共基础知识每日一练（49期） 公共基础知识每日一练（50期） 长按二维码关注我们 shxsydw ↓↓点击底部阅读原文，查看行测每日一练↓↓\",\"thumbnail\":\"http://img01.sogoucdn.com/net/a/04/link?appid=100520033&url=http://mmbiz.qpic.cn/mmbiz_jpg/z1IzSylB4UU0qM2Lct5A8tntib3AZX8JaQyreZAmk0Hyz2TPv0RTm98ttAiaF0QrYkLnFKPdX5Q7yyzcqlBBFXrw/0?wx_fmt=jpeg\",\"item_id\":\"3421fba128da0550cb4e1e7e3c233cce\",\"like_count\":\"2\",\"update_date\":\"20170112003359\",\"msgType\":\"1\",\"view_count\":\"374\",\"start_date\":\"20170111000000\",\"brief\":\"17省直十五轮线上模考大赛第四轮模考(回复关键字“第四轮”即可参加):1月14日早上9点开始,15日晚上22点结束,16日晚19点...\"}";
//        JSONObject jsonObject = JSON.parseObject(jsonMsg);
//        Params wechat = new RhinoWechatContentDocMapper(jsonObject).map();
//        Params mp = new RhinoWechatMPDocMapper(jsonObject).map();
//
//        System.out.println(wechat);
//        System.out.println(mp);

        long mainEndTime = System.currentTimeMillis();
        System.out.println(RuntimeUtil.PID + " Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
        System.exit(0);
    }


}
