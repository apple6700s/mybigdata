package com.dt.mig.sync.flush;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.MigEsWeiboCntWriter;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.NestedStructureUtil;
import com.dt.mig.sync.utils.NewtonInterpolationUtil;
import com.dt.mig.sync.utils.SparkUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * com.dt.mig.sync.flush.FlushIsRealEsSpark
 *
 * @author abel.chan
 * @since 2017/4/6
 */
public class FlushTrendEsSpark implements Serializable {
    public static final String SEP = "#";

    public String query = "{\"fields\":[\"uid\",\"reposts_count\",\"comments_count\",\"attitudes_count\",\"post_time\"], \"query\":{\"bool\":{\"should\":[{\"bool\":{\"must\":[{\"missing\":{\"field\":\"attitudes\"}},{\"range\":{\"attitudes_count\":{\"from\":1}}}]}},{\"bool\":{\"must\":[{\"missing\":{\"field\":\"reposts\"}},{\"range\":{\"reposts_count\":{\"from\":1}}}]}},{\"bool\":{\"must\":[{\"missing\":{\"field\":\"comments\"}},{\"range\":{\"comments_count\":{\"from\":1}}}]}}]}}" + "}";
    public int cores = 100;

    public void setQuery(String query) {
        this.query = query;
    }

    public void run() throws Exception {
        String appName = "dbamp-" + this.getClass().getSimpleName();
        StrParams sparkConf = new StrParams();

        System.out.println(query);
        sparkConf.put("es.resource", MigSyncConsts.ES_WEIBO_WRITER_INDEX + "/" + MigSyncConsts.ES_WEIBO_CHILD_TYPE);
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", query);
        sparkConf.put("es.scroll.size", "5000");
        sparkConf.put("spark.executor.memory", "2500m");

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);
        final Accumulator<Integer> totalAcc = jsc.accumulator(0);
        final Accumulator<Integer> errorAcc = jsc.accumulator(0);
        final Accumulator<Integer> wbAcc = jsc.accumulator(0);
        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, Map<String, Object>> tpl2) throws Exception {

                    String mid = tpl2._1();
                    String uid = parseField(BanyanTypeUtil.parseString(tpl2._2().get("uid")));
                    Integer repostsCount = BanyanTypeUtil.parseInt(parseField(BanyanTypeUtil.parseString(tpl2._2().get("reposts_count"))));
                    if (repostsCount == null) {
                        repostsCount = -1;
                    }
                    Integer commentsCount = BanyanTypeUtil.parseInt(parseField(BanyanTypeUtil.parseString(tpl2._2().get("comments_count"))));
                    if (commentsCount == null) {
                        commentsCount = -1;
                    }
                    Integer attitudesCount = BanyanTypeUtil.parseInt(parseField(BanyanTypeUtil.parseString(tpl2._2().get("attitudes_count"))));
                    if (attitudesCount == null) {
                        attitudesCount = -1;
                    }
                    Long postTime = BanyanTypeUtil.parseLong(parseField(BanyanTypeUtil.parseString(tpl2._2().get("post_time"))));

                    List<Tuple2<String, String>> ret = new LinkedList<>();
                    if (valid(uid) && validDate(postTime)) {
                        ret.add(new Tuple2<>(uid, uid + SEP + mid + SEP + postTime + SEP + repostsCount + SEP + commentsCount + SEP + attitudesCount));
                    }
                    return ret;
                }
            })
//                    .groupByKey()
                    .repartition(cores * 3).mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, Integer>() {
                @Override
                public Iterable<Integer> call(Iterator<Tuple2<String, String>> tplIter) throws Exception {
                    MigEsWeiboCntWriter writer = MigEsWeiboCntWriter.getInstance();
                    List<Integer> ret = new LinkedList<>();

                    while (tplIter.hasNext()) {
                        try {
                            totalAcc.add(1);
                            String mergeId = tplIter.next()._2();
//                                Iterable<String> ids = tplIter.next()._2();
//                                for (String mergeId : ids) {
                            String[] arr = mergeId.split(SEP);
                            String uid = arr[0];
                            String mid = arr[1];
                            Long postTime = Long.parseLong(arr[2]);
                            Integer repostsCount = Integer.parseInt(arr[3]);
                            Integer commentsCount = Integer.parseInt(arr[4]);
                            Integer attitudesCount = Integer.parseInt(arr[5]);
                            if (repostsCount <= 0 && commentsCount <= 0 && attitudesCount <= 0) {
                                continue;
                            }
                            Params wb = new Params();
                            wb.put("id", mid);
                            wb.put("mid", mid);
                            if (repostsCount > 0) {
                                List<Map<String, Object>> repostsTrendValues = createTrendValue(mid, postTime, repostsCount);
                                wb.put("reposts", repostsTrendValues);
                            }
                            if (commentsCount > 0) {
                                List<Map<String, Object>> commentsTrendValues = createTrendValue(mid, postTime, commentsCount);
                                wb.put("comments", commentsTrendValues);
                            }
                            if (attitudesCount > 0) {
                                List<Map<String, Object>> attitudesTrendValues = createTrendValue(mid, postTime, attitudesCount);
                                wb.put("attitudes", attitudesTrendValues);
                            }
                            wbAcc.add(1);
//                                    System.out.println("[wb] " + wb.toJson());
                            YZDoc yzDoc = new YZDoc(wb);
                            writer.updateData(yzDoc, uid);
                        } catch (Exception e) {
                            errorAcc.add(1);
                            e.printStackTrace();
                        }
                    }

                    MigEsWeiboCntWriter.getInstance().flush();
                    return ret;
                }
            }).count();

            System.out.println("[ACC TOTAL] " + totalAcc.value());
            System.out.println("[ACC ERROR] " + errorAcc.value());
            System.out.println("[ACC WEIBO] " + wbAcc.value());
        } finally {
            jsc.close();
        }
    }

    public String parseField(String value) {
        if (StringUtils.isEmpty(value) || "null".equals(value)) return null;
        if (value.startsWith("[")) {
            value = value.substring(1, value.length() - 1);
        }
        return value;

    }

    public static boolean valid(String id) {
        return !StringUtil.isNullOrEmpty(id) && !"null".equals(id);
    }

    public static boolean validDate(long date) {
        try {
            new Date(date);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean valid(Params p) {
        return p != null && !p.isEmpty();
    }

    public static void main(String[] args) throws Exception {
//        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
//        FlushTrendEsSpark runner = new FlushTrendEsSpark();
//        runner.run();
//        System.out.println("[PROGRAM] Program exited.");

        System.out.println(createTrendValue("3938453723392452", 1454484002000L, 300));
        // getTrendDist();
        FlushTrendEsSpark flushTrendEsSpark = new FlushTrendEsSpark();
        flushTrendEsSpark.run();
    }


    public static final int TREAND_MAX_VERSION = 30;

//    public static void getTrendDist() {
//        List<String> midCache = Arrays.asList("3981346932585597", "3981346923318352", "3981346894240066");
//        TrendHBaseReader trendHBaseReader = new TrendHBaseReader();
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//
//        //获取点赞分布,评论分布,转发分布
//        for (int i = 0; i < midCache.size(); i++) {
//            try {
//                String mid = midCache.get(i);
//                System.out.println("开始计算mid:" + mid + "的趋势分布!");
//                Params params = trendHBaseReader.readTrend(HBaseUtils.wbTrendPK(mid), TREAND_MAX_VERSION);
//
//                if (params != null && params.containsKey("data")) {
//                    Map<Long, byte[]> datas = (Map<Long, byte[]>) params.get("data");
//                    //日期到 时间戳 到 趋势值, 为了合并每天最晚时间的值。
//                    Map<String, Pair<Long, String>> dateToTimestampToTrend = new TreeMap<>();
//                    for (Map.Entry<Long, byte[]> entry : datas.entrySet()) {
//                        try {
//                            Long timestamp = entry.getKey();
//                            String date = sdf.format(new Date(timestamp));
//                            String trendValue = entry.getValue() != null ? new String(entry.getValue()) : "";
//                            //System.out.println(timestamp + ":::" + trendValue);
//                            if (StringUtils.isNotEmpty(trendValue) && StringUtils.isNotEmpty(date)) {
//                                if (!dateToTimestampToTrend.containsKey(date) || dateToTimestampToTrend.get(date).getFirst() < timestamp) {
//                                    dateToTimestampToTrend.put(date, new Pair<Long, String>(timestamp, trendValue));
//                                }
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    //System.out.println("Map:" + dateToTimestampToTrend);
//                    int prefixAttitudeTotal = 0;
//                    int prefixRepostTotal = 0;
//                    int prefixCommentTotal = 0;
//
//                    List<Map<String, Object>> listReposts = new ArrayList<Map<String, Object>>();
//                    List<Map<String, Object>> listComments = new ArrayList<Map<String, Object>>();
//                    List<Map<String, Object>> listAttitudes = new ArrayList<Map<String, Object>>();
//                    //生成对应的点赞分布,评论分布、转发分布
//                    for (Map.Entry<String, Pair<Long, String>> entry : dateToTimestampToTrend.entrySet()) {
//                        String updateDate = entry.getKey();
//                        String trendValue = entry.getValue().getSecond();
//                        Triple<Integer, Integer, Integer> trendValue1 = WeiboUtil.getTrendValue(trendValue);
//                        //System.out.println("[TREND VALUE]:" + trendValue1);
//                        Integer repostValue = trendValue1.getFirst();
//                        Integer commentValue = trendValue1.getSecond();
//                        Integer attitudeValue = trendValue1.getThird();
//                        NestedStructureUtil.parseTrend(listReposts, updateDate, repostValue - prefixRepostTotal > 0 ? repostValue - prefixRepostTotal : 0);
//                        NestedStructureUtil.parseTrend(listComments, updateDate, commentValue - prefixCommentTotal > 0 ? commentValue - prefixCommentTotal : 0);
//                        NestedStructureUtil.parseTrend(listAttitudes, updateDate, attitudeValue - prefixAttitudeTotal > 0 ? attitudeValue - prefixAttitudeTotal : 0);
//                        prefixAttitudeTotal = attitudeValue;
//                        prefixCommentTotal = commentValue;
//                        prefixRepostTotal = repostValue;
//                    }
//                    System.out.printf("[TREND]mid:%s,repost:%s,comment:%s,attitude:%s\n", mid, listReposts, listComments, listAttitudes);
//                } else {
//                    System.out.println(params == null ? "param等于空!mid:" + mid : params.toJson());
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        }
//    }


    public static List<Map<String, Object>> createTrendValue(String mid, Long posttime, int trendValue) {

        try {
            //生成多少个趋势分布
            long dayTime = Math.abs(posttime - Long.parseLong(mid)) % 7 + 1;

            //用于插值时最终的X分布
            long endSub = Long.parseLong(mid) % 100 + 1;
            if (endSub < dayTime) {
                endSub = endSub + dayTime;
            }

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            double startX = Double.parseDouble(sdf.format(new Date(posttime))) - 1;
            double endX = startX + endSub;
            double[] trainX = new double[]{startX, endX};


            double[] trainY = new double[]{0, trendValue};
            double[] aimX = new double[(int) dayTime];
            for (int i = 0; i < aimX.length; i++) {
                aimX[i] = startX + i + 1;
            }
            DecimalFormat df = new DecimalFormat("0");

            List<Integer> list = NewtonInterpolationUtil.Newton_inter_method(trainX, trainY, aimX, trendValue);

            if (list != null && list.size() <= aimX.length) {
                List<Map<String, Object>> maps = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    NestedStructureUtil.parseTrend(maps, df.format(aimX[i]), list.get(i));
                }
                return maps;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return null;
    }


}
