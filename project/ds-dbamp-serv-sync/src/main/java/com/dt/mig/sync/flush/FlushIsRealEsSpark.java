package com.dt.mig.sync.flush;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.MigEsWeiboCntWriter;
import com.dt.mig.sync.hbase.HBaseReader;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.SparkUtil;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * com.dt.mig.sync.flush.FlushIsRealEsSpark
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class FlushIsRealEsSpark implements Serializable {
    public static final String INF_STARTDATE = "19000101";
    public static final String INF_ENDDATE = "30000101";
    public static final String SEP = "#";
    public static final byte[] R = "r".getBytes();
    public static final int BATCH = 10000;
    public static final String QUERY_WRAPPER = "{\"fields\":[], \"query\":{%s}}";
    public static final String QUERY_TEMPLATE = "{\n" + "    \"fields\":[],\n" + "   \"query\": {\n" + "      \"has_child\": {\n" + "         \"type\": \"weibo\",\n" + "         \"query\": {\n" + "            \"bool\": {\n" + "               \"must\": [\n" + "                  {\n" + "                     \"range\": {\n" + "                        \"post_time_date\": {\n" + "                           \"from\": \"%s\",\n" + "                           \"to\": \"%s\"\n" + "                        }\n" + "                     }\n" + "                  }\n" + "               ]\n" + "            }\n" + "         }\n" + "      }\n" + "   }\n" + "}";
    public String query = "{\"fields\":[\"uid\"], \"query\":{\"missing\":{\"field\":\"is_real\"}}}";
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
        final Accumulator<Integer> userAcc = jsc.accumulator(0);
        final Accumulator<Integer> wbAcc = jsc.accumulator(0);
        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, Map<String, Object>> tpl2) throws Exception {
                    String mid = tpl2._1();
                    String uid = parseField(BanyanTypeUtil.parseString(tpl2._2().get("uid")));
                    List<Tuple2<String, String>> ret = new LinkedList<>();
                    if (valid(uid)) {
                        ret.add(new Tuple2<>(uid, uid + SEP + mid));
                    }
                    return ret;
                }
            })
//                    .groupByKey()
                    .repartition(cores * 3)
//                    .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Iterable<String>>>, byte[]>() {
//                        @Override
//                        public Iterable<byte[]> call(Iterator<Tuple2<String, Iterable<String>>> tplIter) throws Exception {
                    .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, Integer>() {
                        @Override
                        public Iterable<Integer> call(Iterator<Tuple2<String, String>> tplIter) throws Exception {
                            HBaseReader hBaseReader = new HBaseReader() {
                                @Override
                                public String getTable() {
                                    return "DS_BANYAN_WEIBO_USER";
                                }
                            };
                            hBaseReader.setMaxCache(2000);
                            HashMap<String, List<String>> mp = new HashMap<>();
                            List<Integer> ret = new LinkedList<>();

                            while (tplIter.hasNext()) {
                                totalAcc.add(1);
                                String mergeId = tplIter.next()._2();
//                                Iterable<String> ids = tplIter.next()._2();
//                                for (String mergeId : ids) {
                                String[] arr = mergeId.split(SEP);
                                String uid = arr[0];
                                String mid = arr[1];
                                if (!mp.containsKey(uid)) mp.put(uid, new LinkedList<String>());
                                mp.get(uid).add(mid);
                                Get get = buildUserGet(uid);
                                List<Params> hbRes = hBaseReader.batchRead(get);
                                if (!CollectionUtil.isEmpty(hbRes)) {
                                    flush(hbRes, mp);
                                }
//                                }
                            }
                            List<Params> hbRes = hBaseReader.flush();
                            if (!CollectionUtil.isEmpty(hbRes)) {
                                flush(hbRes, mp);
                            }

                            MigEsWeiboCntWriter.getInstance().flush();
                            return ret;
                        }

                        public List<byte[]> flush(List<Params> hbRes, HashMap<String, List<String>> mp) {
                            List<byte[]> ret = new LinkedList<byte[]>();
                            MigEsWeiboCntWriter writer = MigEsWeiboCntWriter.getInstance();
                            for (Params u : hbRes) {
                                if (!valid(u)) continue;

                                String uid = u.getString("uid");
                                int isReal = isReal(u.getString("user_type"));
                                List<String> midlist = mp.get(uid);
                                userAcc.add(1);

                                if (!CollectionUtil.isEmpty(midlist)) for (String mid : midlist) {
                                    try {
                                        wbAcc.add(1);
                                        Params wb = new Params();
                                        wb.put("id", mid);
                                        wb.put("mid", mid);
                                        wb.put("is_real", isReal);

                                        YZDoc yzDoc = new YZDoc(wb);
                                        writer.updateData(yzDoc, uid);
//                                            wb.put("uid", uid);
//                                            ret.add(ProtostuffSerializer.serializeObject(new Params.ParamsPojo(wb)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            hbRes.clear();
                            mp.clear();
                            return ret;
                        }
                    }).count()
//                    .repartition(cores + 20)
//                    .foreachPartition(new VoidFunction<Iterator<byte[]>>() {
//                        @Override
//                        public void call(Iterator<byte[]> iterator) throws Exception {
//                            MigEsWeiboCntWriter writer = MigEsWeiboCntWriter.getInstance();
//                            while (iterator.hasNext()) {
//                                try {
//                                    byte[] bs = iterator.next();
//                                    Params.ParamsPojo pojo = ProtostuffSerializer.deserialize(bs, Params.ParamsPojo.class);
//                                    Params p = pojo.getP();
//                                    String uid = p.getString("uid");
//                                    p.remove("uid");
//                                    YZDoc yzDoc = new YZDoc(p);
//                                    writer.addData(yzDoc, uid);
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//                            }
//                            writer.flush();
//                        }
//                    })
            ;

            System.out.println("[ACC TOTAL] " + totalAcc.value());
            System.out.println("[ACC USER] " + userAcc.value());
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

    public static Get buildUserGet(String uid) {
        if (StringUtil.isNullOrEmpty(uid)) return null;
        String pk = BanyanTypeUtil.wbuserPK(uid);
        Get get = new Get(pk.getBytes());
        get.addColumn(R, "uid".getBytes());
        get.addColumn(R, "user_type".getBytes());

        return get;
    }

    public static boolean valid(String id) {
        return !StringUtil.isNullOrEmpty(id) && !"null".equals(id);
    }

    public static boolean valid(Params p) {
        return p != null && !p.isEmpty();
    }

    public static int isReal(String userType) {
        if ("2".equals(userType)) return 0;
        else return 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        FlushIsRealEsSpark runner = new FlushIsRealEsSpark();
        runner.run();
        System.out.println("[PROGRAM] Program exited.");
    }

}
