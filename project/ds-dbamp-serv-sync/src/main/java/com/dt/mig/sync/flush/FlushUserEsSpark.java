package com.dt.mig.sync.flush;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonReader;
import com.dt.mig.sync.es.MigEsWeiboUserWriter;
import com.dt.mig.sync.hbase.HBaseReader;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.SparkUtil;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Get;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.flush.FlushUserEsSpark
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class FlushUserEsSpark implements Serializable {
    public static final String INF_STARTDATE = "19000101";
    public static final String INF_ENDDATE = "30000101";
    public static final byte[] R = "r".getBytes();
    public static final int BATCH = 10000;
    public static String startDate;
    public static String endDate;
    public static final String QUERY_WRAPPER = "{\"fields\":[], \"query\":{%s}}";
    public static final String QUERY_TEMPLATE = "{\n" + "    \"fields\":[],\n" + "   \"query\": {\n" + "      \"has_child\": {\n" + "         \"type\": \"weibo\",\n" + "         \"query\": {\n" + "            \"bool\": {\n" + "               \"must\": [\n" + "                  {\n" + "                     \"range\": {\n" + "                        \"post_time_date\": {\n" + "                           \"from\": \"%s\",\n" + "                           \"to\": \"%s\"\n" + "                        }\n" + "                     }\n" + "                  }\n" + "               ]\n" + "            }\n" + "         }\n" + "      }\n" + "   }\n" + "}";
    public String query = "{\"fields\":[]}";
    public int cores = 100;

    public void setRangeDate(String startDate, String endDate) {
        setQuery(String.format(QUERY_TEMPLATE, startDate, endDate));
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void scrollUID(FileSystem fs) {
        CommonReader reader = CommonReader.getInstance();
        QueryBuilder qb = QueryBuilders.queryStringQuery(query);
    }

    public void run() throws Exception {
        String appName = "dbamp-" + this.getClass().getSimpleName() + "-" + startDate + ":" + endDate;
        StrParams sparkConf = new StrParams();

        System.out.println(query);
        sparkConf.put("es.resource", MigSyncConsts.ES_WEIBO_WRITER_INDEX + "/" + MigSyncConsts.ES_WEIBO_PARENT_TYPE);
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", query);
        sparkConf.put("es.scroll.size", "5000");

//        String root = "/tmp/mig";
//        Path rp = new Path(root);
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(rp))
//            fs.delete(rp);
//        if (!fs.exists(rp))
//            fs.mkdirs(rp);

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);
        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            esRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Object>>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, Map<String, Object>> tpl2) throws Exception {
                    String id = tpl2._1();
                    return Arrays.asList(new Tuple2<>(Md5Util.md5(id).substring(0, 3), id));
                }
            }).groupByKey(cores).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Iterable<String>>> tplIter) throws Exception {
                    HBaseReader hBaseReader = new HBaseReader() {
                        @Override
                        public String getTable() {
                            return "DS_BANYAN_WEIBO_USER";
                        }
                    };
                    hBaseReader.setMaxCache(100);

                    while (tplIter.hasNext()) {
                        Iterable<String> ids = tplIter.next()._2();
                        for (String uid : ids) {
                            Get get = buildUserGet(uid);
                            List<Params> hbRes = hBaseReader.batchRead(get);
                            if (!CollectionUtil.isEmpty(hbRes)) {
                                flush(hbRes);
                            }
                        }
                    }
                    List<Params> hbRes = hBaseReader.flush();
                    if (!CollectionUtil.isEmpty(hbRes)) {
                        flush(hbRes);
                    }

                    MigEsWeiboUserWriter.getInstance().flush();
                }

                public void flush(List<Params> hbRes) {
                    MigEsWeiboUserWriter writer = MigEsWeiboUserWriter.getInstance();
                    for (Params u : hbRes) {
                        if (u == null || u.isEmpty()) continue;

                        List<String> metaGroup = BanyanTypeUtil.yzStr2List(u.getString("meta_group"));
                        if (!CollectionUtil.isEmpty(metaGroup)) {
                            int size = Math.min(metaGroup.size(), 100);
                            metaGroup = metaGroup.subList(1, size);
                        }

                        List<String> topics = BanyanTypeUtil.yzStr2List(u.getString("topics"));
                        if (!CollectionUtil.isEmpty(topics)) {
                            if (topics.size() > 100) topics = topics.subList(0, 100);
                        }

                        u.put("id", u.getString("uid"));
                        u.remove("uid");
                        u.put("meta_group", metaGroup);
                        u.put("topics", topics);
                        YZDoc yzDoc = new YZDoc(u);
                        try {
                            writer.updateData(yzDoc);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    hbRes.clear();
                }
            });
        } finally {
            jsc.close();
            jsc.stop();
        }

    }

    public static Get buildUserGet(String uid) {
        if (StringUtil.isNullOrEmpty(uid)) return null;
        String pk = BanyanTypeUtil.wbuserPK(uid);
        Get get = new Get(pk.getBytes());
        get.addColumn(R, "uid".getBytes());
        get.addColumn(R, "meta_group".getBytes());
        get.addColumn(R, "topics".getBytes());

        return get;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        FlushUserEsSpark runner = new FlushUserEsSpark();
        if (args.length > 1) runner.setRangeDate(args[0], args[1]);
        runner.run();
        System.out.println("[PROGRAM] Program exited.");
    }

}
