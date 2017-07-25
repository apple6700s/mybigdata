package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.datastory.banyan.utils.BanyanTypeUtil.parseString;
import static com.datastory.banyan.utils.BanyanTypeUtil.valid;

/**
 * com.datastory.banyan.migrate1.tools.FixRetweetContent
 *
 * @author lhfcws
 * @since 2017/2/24
 */
public class FixRetweetContent extends AbstractSparkRunner {
    public static final String SEP = "#";
    public static final byte[] R = "r".getBytes();
    static Configuration conf = RhinoETLConfig.getInstance();

    public String query = "{\n" +
            "  \"fields\": [\n" +
            "    \"retweet_id\"\n" +
            "  ],\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        {\n" +
            "          \"missing\": {\n" +
            "            \"field\": \"retweet_content\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"exists\": {\n" +
            "            \"field\": \"retweet_id\"\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    public int cores = 300;

    public StrParams buildEsSparkConf() {
        StrParams sparkConf = super.customizedSparkConfParams();
        System.out.println(query);
        sparkConf.put("es.resource", "dt-rhino-weibo-v2/weibo");
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", query);
        sparkConf.put("es.scroll.size", "5000");
        sparkConf.put("es.nodes.wan.only", "true");
        sparkConf.put("spark.executor.memory", "6500m");
        return sparkConf;
    }

    public String parseField(String value) {
        if (StringUtils.isEmpty(value) || "null".equals(value))
            return null;
        if (value.startsWith("[")) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    public Get buildGet(String mid) {
        String pk = BanyanTypeUtil.wbcontentPK(mid);
        Get get = new Get(pk.getBytes());
        get.addColumn(R, "mid".getBytes());
        get.addColumn(R, "content".getBytes());
        return get;
    }

    public void run() {
        String appName = this.getClass().getSimpleName();
        sparkConf = buildEsSparkConf();
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores + "", sparkConf);
        final Accumulator<Integer> writeAcc = jsc.accumulator(0);
        final Accumulator<Integer> readAcc = jsc.accumulator(0);
        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, String>() {
                        @Override
                        public Iterable<Tuple2<String, String>> call(Tuple2<String, Map<String, Object>> tpl2) throws Exception {
                            String mid = tpl2._1();
                            String srcMid = parseField(parseString(tpl2._2().get("retweet_id")));
                            List<Tuple2<String, String>> ret = new LinkedList<>();
                            if (valid(srcMid)) {
                                ret.add(new Tuple2<>(
                                        BanyanTypeUtil.sub3PK(mid).substring(0, 3), mid
                                ));
                                readAcc.add(1);
                            }
                            return ret;
                        }
                    })
                    .groupByKey(cores * 10)
//                    .foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
//                        @Override
//                        public void call(Iterator<Tuple2<String, Iterable<String>>> iter) throws Exception {
//                            HBaseReader hBaseReader = new HBaseReader() {
//                                @Override
//                                public String getTable() {
//                                    return Tables.table(Tables.PH_WBCNT_TBL);
//                                }
//                            };
//
//                            RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_WBCNT_TBL));
//
//                            HashMap<String, List<String>> mp = new HashMap<>();
//                            while (iter.hasNext()) {
//                                Tuple2<String, Iterable<String>> tpl2 = iter.next();
//                                String srcMid = tpl2._1();
//                                {
//                                    List<String> mids = new LinkedList<>();
//
//                                    for (String mid : tpl2._2()) {
//                                        mids.add(mid);
//                                    }
//                                    if (!mids.isEmpty())
//                                        mp.put(srcMid, mids);
//                                }
//
//                                Get get = buildGet(srcMid);
//                                List<Params> hbRes = hBaseReader.batchRead(get);
//                                if (validate(hbRes)) {
//                                    for (Params p : hbRes)
//                                        if (validate(p)) {
//                                            String srcMid_ = p.getString("mid");
//                                            List<String> mids = mp.get(srcMid_);
//                                            if (!validate(mids))
//                                                continue;
//
//                                            String content = p.getString("content");
//                                            if (validate(content)) {
//                                                for (String mid : mids) {
//                                                    Params cnt = new Params();
//                                                    cnt.put("pk", BanyanTypeUtil.wbcontentPK(mid));
//                                                    cnt.put("src_content", content);
//                                                    writeAcc.add(1);
//                                                    putter.batchWrite(cnt);
//                                                }
//                                            }
//                                        }
//                                }
//                            }
//
//                            List<Params> hbRes = hBaseReader.flush();
//                            if (validate(hbRes)) {
//                                for (Params p : hbRes)
//                                    if (validate(p)) {
//                                        String srcMid_ = p.getString("mid");
//                                        List<String> mids = mp.get(srcMid_);
//                                        if (!validate(mids))
//                                            continue;
//
//                                        String content = p.getString("content");
//                                        if (validate(content)) {
//                                            for (String mid : mids) {
//                                                Params cnt = new Params();
//                                                cnt.put("pk", BanyanTypeUtil.wbcontentPK(mid));
//                                                cnt.put("src_content", content);
//                                                writeAcc.add(1);
//                                                putter.batchWrite(cnt);
//                                            }
//                                        }
//                                    }
//                            }
//                            putter.flush();
//                        }
//                    })
            .foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Iterable<String>>> tuple2Iterator) throws Exception {
                    
                }
            })
            ;

            System.out.println("");
            System.out.println("[TOTAL] READ = " + readAcc.value());
            System.out.println("[TOTAL] WRITE = " + writeAcc.value());
        } finally {
            jsc.close();
            jsc.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        FixRetweetContent fixRetweetContent = new FixRetweetContent();
        fixRetweetContent.run();
        System.out.println("[PROGRAM] Program exited.");
    }
}
