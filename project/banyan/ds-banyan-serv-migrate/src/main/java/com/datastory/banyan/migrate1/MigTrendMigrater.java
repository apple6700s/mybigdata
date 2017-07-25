package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


/**
 * com.datastory.banyan.migrate1.MigTrendMigrater
 *
 * @author lhfcws
 * @since 2017/4/6
 */
public class MigTrendMigrater extends AbstractSparkRunner {
    protected static Logger LOG = Logger.getLogger(MigTrendMigrater.class);
    public static final String MIG_ES = "dt-rhino-weibo-mig-v5";
//    public static final String QUERY = "{\"size\":1,\"fields\":[\"comments.update_date\",\"comments.count\",\"reposts.update_date\",\"reposts.count\",\"attitudes.update_date\",\"attitudes.count\"],\"query\":{\"match_all\":{}}}";
    public static final String QUERY = "{\"fields\":[\"comments.update_date\",\"comments.count\",\"reposts.update_date\",\"reposts.count\",\"attitudes.update_date\",\"attitudes.count\"],\"query\":{\"match_all\":{}}}";

    public StrParams buildEsSparkConf() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("es.resource", MIG_ES + "/weibo");
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.scroll.size", "5000");
        sparkConf.put("es.nodes.wan.only", "true");
        sparkConf.put("es.query", QUERY);
        sparkConf.put("spark.executor.memory", "3500m");
        return sparkConf;
    }

    public void run() {
        String appName = this.getClass().getSimpleName();
        sparkConf = buildEsSparkConf();
        cores = sparkConf.get("es.nodes").split(",").length;
        cores = 10;
        sparkConf.put("spark.cores.max", cores + "");
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores + "", sparkConf);
        final Accumulator<Integer> filterAcc = jsc.accumulator(0);
        final Accumulator<Integer> readAcc = jsc.accumulator(0);
        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Map<String, Object>>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Map<String, Object>>> iter) throws Exception {
                    RFieldPutter writer = new RFieldPutter(Tables.table(Tables.PH_TREND_TBL));

                    while (iter.hasNext()) {
                        Tuple2<String, Map<String, Object>> tpl = iter.next();
                        String id = tpl._1();
                        Map<String, Object> sources = tpl._2();
//                        LOG.info(id + " : " + sources);
                        readAcc.add(1);

                        if (sources != null) {
                            ArrayList<RCA> trend = new ArrayList<>();

                            for (Map.Entry<String, Object> e : sources.entrySet()) {
                                String key = e.getKey();
                                if (key.endsWith("update_date")) {
                                    ArrayList list = (ArrayList) e.getValue();
                                    for (Object obj : list) {
                                        String dateStr = String.valueOf(obj);
//                                        Date date = DateUtils.parse(dateStr, DateUtils.DFT_DAYFORMAT);
//                                        long ts = date.getTime();
                                        RCA rca = new RCA(dateStr);
                                        trend.add(rca);
                                    }
                                    break;
                                }
                            }

                            if (!trend.isEmpty()) {
                                for (Map.Entry<String, Object> e : sources.entrySet()) {
                                    String key = e.getKey();
                                    String distKey = key.split("\\.")[0];

                                    if (key.endsWith("count")) {
                                        ArrayList list = (ArrayList) e.getValue();
                                        int i = -1;
                                        for (Object obj : list) {
                                            i++;
                                            Long v = (Long) obj;

                                            try {
                                                RCA rca = trend.get(i);
                                                rca.set(distKey, v);
                                            } catch (Exception exc) {
                                                LOG.error("[Index Align Error] " + exc.getMessage());
                                            }
                                        }
                                    }
                                }

                                String pk = RhinoETLConsts.SRC_WB + "|" + id + "|rca";
                                Params p = new Params("pk", pk);
                                p.put("source", RhinoETLConsts.SRC_WB);
                                p.put("type", "rca");
//                                p.put("parent_id", uid);

                                for (RCA rca : trend) {
                                    if (rca.updateDate != null) {
                                        Params out = new Params(p);
                                        out.put("update_date", rca.updateDate);
                                        out.put("data", rca.toString());

                                        HBaseUtils.writeTrend(writer, out);
                                    }
                                }

                            }
                            LOG.info(id + " => " + trend);
                        } else
                            filterAcc.add(1);
                    }
                    writer.flush(true);
                }
            });

            System.out.println("\n");
            System.out.println("[ACC] read=" + readAcc.value());
            System.out.println("[ACC] filter=" + filterAcc.value());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    public static class RCA implements Serializable {
//        private long timestamp;
        String updateDate = null;
        private long reposts = 0;
        private long comments = 0;
        private long attitudes = 0;

//        public RCA(long timestamp) {
//            this.timestamp = timestamp;
//        }

        public RCA(String updateDate) {
            if (updateDate.length() == 14)
                this.updateDate = updateDate;
            else if (updateDate.length() == 8)
                this.updateDate = updateDate + "000000";
        }

        public long getReposts() {
            return reposts;
        }

        public void setReposts(long reposts) {
            this.reposts = reposts;
        }

        public long getComments() {
            return comments;
        }

        public void setComments(long comments) {
            this.comments = comments;
        }

        public long getAttitudes() {
            return attitudes;
        }

        public void setAttitudes(long attitudes) {
            this.attitudes = attitudes;
        }

        public void set(String key, long v) {
            if (key.equals("reposts"))
                setReposts(v);
            else if (key.equals("comments"))
                setComments(v);
            else if (key.equals("attitudes"))
                setAttitudes(v);
        }

        @Override
        public String toString() {
            return reposts + "|" + comments + "|" + attitudes;
        }
    }

    public static void main(String[] args) throws Exception {
        MigTrendMigrater runner = new MigTrendMigrater();
        runner.run();
        System.out.println("[PROGRAM] Program exited.");
    }
}
