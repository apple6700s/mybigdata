package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.WbCntESWriter;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * com.datastory.banyan.migrate1.ReflushESSpark
 *
 * @author lhfcws
 * @since 2016/12/30
 */
public class ReflushESSpark implements Serializable {
    static Logger LOG = Logger.getLogger(ReflushESSpark.class);

    public int getCores() {
        return 1;
    }

    public void run() {
        StrParams sparkConf = new StrParams();
        RhinoETLConfig conf = RhinoETLConfig.getInstance();
        sparkConf.put("es.resource", "dt-rhino-weibo-index/weibo");
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", "{\n" +
                "   \"query\": {\n" +
                "      \"bool\": {\n" +
                "         \"must\": [\n" +
                "            {\n" +
                "        \"term\": {\n" +
                "           \"id\": {\n" +
                "              \"value\": \"4076180736549571\"\n" +
                "           }\n" +
                "        }\n" +
                "    }\n" +
                "         ]\n" +
                "      }\n" +
                "   }\n" +
                "}");
        sparkConf.put("es.scroll.size", "200");
        sparkConf.put("es.mapping.parent", "parent");
        sparkConf.put("es.mapping.routing", "_routing");

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, "ReflushESSpark", getCores() + "", sparkConf);
        final Accumulator<Integer> accu = jsc.accumulator(0, "X");

        String hdfs = "/tmp/es-reflush";

        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.map(new Function<Tuple2<String, Map<String, Object>>, String>() {
                @Override
                public String call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                    accu.add(1);
                    System.out.println(v1._1());
                    System.out.println(v1._2());
                    return v1._1();
                }
            }).collect();

//            final String table = Tables.table(Tables.PH_WBCNT_TBL);
//            jsc
//                    .textFile(hdfs)
//                    .map(new Function<String, String>() {
//                        @Override
//                        public String call(String mid) throws Exception {
//                            String pk = BanyanTypeUtil.wbcontentPK(mid);
//                            return pk;
//                        }
//                    })
//                    .repartition(getCores())
//                    .foreachPartition(new VoidFunction<Iterator<String>>() {
//                        @Override
//                        public void call(Iterator<String> mIter) throws Exception {
//                            HBaseReader reader = new HBaseReader() {
//                                @Override
//                                public String getTable() {
//                                    return table;
//                                }
//                            };
//                            WbCntESWriter esWriter = WbCntESWriter.getInstance();
//
//                            long size = 0;
//                            while (mIter.hasNext()) {
//                                size++;
//                                String pk = mIter.next();
//                                List<Params> res = reader.batchRead(pk);
//                                if (CollectionUtil.isNotEmpty(res)) {
//                                    if (size % 1000 == 0)
//                                        System.out.println("[SAMPLE] pk : " + pk + ", flush size: " + res.size() + ", partition progress: " + size);
//                                    for (Params hbDoc : res) {
//                                        Params esDoc = new WbCnt2RhinoESDocMapper(hbDoc).map();
//                                        esWriter.write(esDoc);
//                                    }
//                                }
//                            }
//                            List<Params> res = reader.flush();
//                            if (CollectionUtil.isNotEmpty(res)) {
//                                for (Params hbDoc : res) {
//                                    Params esDoc = new WbCnt2RhinoESDocMapper(hbDoc).map();
//                                    esWriter.write(esDoc);
//                                }
//                            }
//                            esWriter.flush();
//                        }
//                    })
//            ;
//            System.out.println("Total: " + accu.value());
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        ReflushESSpark esSpark = new ReflushESSpark();
        esSpark.run();
        System.out.println("[PROGRAM] Program exited.");
    }
}
