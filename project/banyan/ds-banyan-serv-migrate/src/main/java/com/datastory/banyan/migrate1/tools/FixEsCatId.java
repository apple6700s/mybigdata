package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;
import com.datastory.banyan.spark.SparkUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.migrate1.tools.FixEsCatId
 *
 * @author lhfcws
 * @since 2017/5/8
 */
public class FixEsCatId implements Serializable {
    public int getCores() {
        return 12;
    }

    public void run() {
        StrParams sparkConf = new StrParams();
        sparkConf.put("es.resource", "dt-rhino-weibo-index/weibo");
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", "{\n" +
                "    \"query\": {\n" +
                "        \"term\": {\n" +
                "           \"cat_id\": {\n" +
                "              \"value\": \"-1\"\n" +
                "           }\n" +
                "        }\n" +
                "    }\n" +
                "}");
        sparkConf.put("es.scroll.size", "200");
        sparkConf.put("es.mapping.parent", "parent");
        sparkConf.put("es.mapping.routing", "_routing");

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, "FixEsCatId", getCores() + "", sparkConf);
        final Accumulator<Integer> accu = jsc.accumulator(0, "Total");
        final String table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);

        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.map(new Function<Tuple2<String, Map<String, Object>>, String>() {
                @Override
                public String call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                    accu.add(1);
                    return v1._1();
                }
            }).repartition(48).foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iter) throws Exception {
                    HBaseReader reader = new HBaseReader() {
                        @Override
                        public String getTable() {
                            return table;
                        }
                    };
                    while (iter.hasNext()) {
                        String pk = iter.next();
                        List<Params> ret = reader.batchRead(pk);
                        if (ret != null) {
                            flush(ret);
                        }
                    }

                    List<Params> ret = reader.flush();
                    if (ret != null) {
                        flush(ret);
                    }
                    NewsForumCmtESWriter.getInstance().awaitFlush();
                }

                public void flush(List<Params> ret) {
                    for (Params p : ret)
                        NewsForumCmtESWriter.getInstance().write(p);
                }
            });

            System.out.println("\nTotal: " + accu.value());
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        FixEsCatId esSpark = new FixEsCatId();
        esSpark.run();
        System.out.println("[PROGRAM] Program exited.");
    }
}
