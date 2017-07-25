package com.dt.mig.sync.weibo;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.es.CommonReader;
import com.dt.mig.sync.es.CommonWriter;
import com.dt.mig.sync.es.MigEsWeiboCntWriter;
import com.dt.mig.sync.extract.MsgTypeExtractor;
import com.dt.mig.sync.extract.SelfContentExtractor;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.SparkUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by abel.chan on 17/2/23.
 */
@Deprecated
public class WeiboUpdateMsgType implements Serializable {

    private final static String index = "dt-rhino-weibo-mig-v6.1";
    private final static String type = "weibo";

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
    private final static String start = "20160601000000";
    private final static String end = "20160603000000";
    private final static int cores = 1;

    public QueryBuilder buildQueryBuilder() throws Exception {
//        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
//        QueryBuilder rangeQuery =
//                QueryBuilders.rangeQuery(MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME)
//                        .from(sdf.parse(start).getTime()).to(sdf.parse(end).getTime());
//        queryBuilder.must(rangeQuery);
//        return queryBuilder;
        return QueryBuilders.termQuery("id", "3981995283779131");
    }

    public void execute() throws Exception {
        QueryBuilder queryBuilder = buildQueryBuilder();
        try {

            long cnt = CommonReader.getInstance().getTotalHits(index, type, queryBuilder);
            System.out.println("[Init Count] " + cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(queryBuilder.toString().replaceAll("\\n", ""));
        String appName = this.getClass().getSimpleName() + "-" + start + ":" + end;
        StrParams sparkConf = new StrParams();

        sparkConf.put("es.resource", index + "/" + type);
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", "{ \"fields\":[\"id\", \"retweet_id\",\"pid\",\"self_content\",\"uid\",\"content\"], \"query\": " + queryBuilder.toString() + " }");
        sparkConf.put("es.scroll.size", "1000");

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);

        final JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
        esRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Map<String, Object>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Map<String, Object>>> iter) throws Exception {
                final CommonWriter cntEsWriter = MigEsWeiboCntWriter.getInstance();
                while (iter.hasNext()) {
                    Tuple2<String, Map<String, Object>> tpl = iter.next();
                    String mid = tpl._1();

                    String srcMid = parseField(BanyanTypeUtil.parseString(tpl._2().get("retweet_id")));
                    String rtMid = parseField(BanyanTypeUtil.parseString(tpl._2().get("pid")));
                    String selfContent = SelfContentExtractor.extract(parseField(BanyanTypeUtil.parseString(tpl._2().get("self_content"))));
                    String content = parseField(BanyanTypeUtil.parseString(tpl._2().get("content")));
                    String uid = parseField(BanyanTypeUtil.parseString(tpl._2().get("uid")));

                    Short msgType = MsgTypeExtractor.analyz(srcMid, rtMid, selfContent, content);
                    System.out.printf("[WRITE]msgType:%s,uid:%s,mid:%s,srcMid:%s,rtMid:%s,selfContent:%s,content:%s.\n", msgType, checkNull(uid), checkNull(mid), checkNull(srcMid), checkNull(rtMid), checkNull(selfContent), checkNull(content));


                    if (selfContent == null) {
                        CommonReader esReader = CommonReader.getInstance();
                        esReader.search(index, type, 0, 0, QueryBuilders.scriptQuery(new Script("ctx._source.remove(\"self_content\")")));
                    }


                    if (StringUtils.isNotEmpty(uid) || "null".equals(uid.trim())) {
                        cntEsWriter.updateDataWithMap(getYzDoc(mid, msgType), uid);
                    }
                }
                cntEsWriter.flush();

            }
        });
    }

    public String checkNull(String value) {
        if (value != null) return value;
        return "i is null!";
    }

    public String parseField(String value) {
        if (StringUtils.isEmpty(value) || "null".equals(value)) return null;
        if (value.startsWith("[")) {
            value = value.substring(1, value.length() - 1);
        }
        return value;

    }

    public YZDoc getYzDoc(String mid, Short msgType) {
        YZDoc yzDoc = new YZDoc();
        yzDoc.put("msg_type", msgType);
        yzDoc.put("id", mid);
        // yzDoc.put("self_content", selfContent);
        return yzDoc;
    }


    public static void main(String[] args) {
        try {
//            YZDoc yzDoc = new YZDoc();
//            yzDoc.put("msg_type", 2);
//            yzDoc.put("id", "3344444");
//            yzDoc.put("self_content", null);
//            System.out.println(yzDoc.toJson());
            WeiboUpdateMsgType bushu = new WeiboUpdateMsgType();
            bushu.execute();
//            bushu.updateEs(datas);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
