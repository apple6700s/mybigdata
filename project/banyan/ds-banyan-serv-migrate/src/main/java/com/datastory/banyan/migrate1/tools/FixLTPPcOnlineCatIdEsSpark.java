package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.weibo.abel.Tables;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;


/**
 * com.datastory.banyan.migrate1.tools.FixLTPPcOnlineCatIdMR
 *
 * @author abel.chan
 * @since 17/06/19
 */

public class FixLTPPcOnlineCatIdEsSpark implements Serializable {


    public static Logger LOG = Logger.getLogger(FixLTPPcOnlineCatIdEsSpark.class);

//    static final String table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);

    static final String table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);

    static final byte[] R = "r".getBytes();

    static final String index = "ds-banyan-newsforum-index";
//    static final String type = "post";
    static final String type = "comment";

    private QueryBuilder buildQueryBuilder() {

        LOG.info("初始化参数开始");
        QueryBuilder queryBuilder = QueryBuilders.termQuery("cat_id", 2);
        LOG.info("初始化参数结束");
//        return QueryBuilders.termQuery("id", "4076096184261816");
        return queryBuilder;
    }

    public void run() throws IOException {
//        QueryBuilder queryBuilder = buildQueryBuilder();
//        System.out.println(queryBuilder.toString().replaceAll("\\n", ""));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String appName = this.getClass().getSimpleName() + "-" +
                sdf.format(new Date());
        StrParams sparkConf = new StrParams();

        System.out.println("[table]:" + table);

        sparkConf.put("es.resource", index + "/" + type);
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", "{ \"fields\":[], \"query\": {\"term\":{\"cat_id\":{\"value\":\"2\"}}}}");
        sparkConf.put("es.scroll.size", "10000");
        String cores = "50";
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);
        final Accumulator<Integer> errorAcc = jsc.accumulator(0);

        final Accumulator<Integer> readAcc = jsc.accumulator(0);

        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            JavaRDD<String> rdd = esRDD
                    .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Map<String, Object>>>, String>() {
                        @Override
                        public Iterable<String> call(Iterator<Tuple2<String, Map<String, Object>>> iter) throws Exception {

                            RFieldPutter putter = new RFieldPutter(table);
                            while (iter.hasNext()) {
                                readAcc.add(1);
                                try {
                                    Tuple2<String, Map<String, Object>> tpl = iter.next();
                                    String pk = tpl._1();

                                    LOG.info("[pk]:" + pk);
                                    if (StringUtils.isNotEmpty(pk)) {
                                        //说明需要重写到hbase
                                        Put put = new Put(pk.getBytes());
                                        //写回2
                                        put.addColumn(R, "cat_id".getBytes(), "2".getBytes());
                                        putter.batchWrite(put);
                                    }

                                } catch (Exception e) {
                                    e.printStackTrace();
                                    errorAcc.add(1);
                                }
                            }
                            putter.flush();
                            return new ArrayList<String>();
                        }
                    });
            rdd.count();
            System.out.println("readAcc:" + readAcc.value());
            System.out.println("errorAcc:" + errorAcc.value());
        } catch (Exception ex) {
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        new FixLTPPcOnlineCatIdEsSpark().run();
        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
