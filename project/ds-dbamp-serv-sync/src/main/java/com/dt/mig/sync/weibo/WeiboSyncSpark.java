package com.dt.mig.sync.weibo;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.es.*;
import com.dt.mig.sync.hbase.HBaseReader;
import com.dt.mig.sync.hbase.HBaseUtils;
import com.dt.mig.sync.hbase.RFieldGetter;
import com.dt.mig.sync.hbase.TrendHBaseReader;
import com.dt.mig.sync.hbase.doc.Doc2UserWrapper;
import com.dt.mig.sync.hbase.doc.WbCnt2MigESDocMapper;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.dt.mig.sync.utils.*;
import com.dt.mig.sync.words.FilterDoc;
import com.dt.mig.sync.words.IWords;
import com.dt.mig.sync.words.KeyWords;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.Triple;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.dt.mig.sync.base.MigSyncConsts.*;

/**
 * com.dt.mig.sync.weibo.WeiboSyncSpark
 *
 * @author lhfcws
 * @since 2017/2/21
 */
public class WeiboSyncSpark implements Serializable {
    public static Logger LOG = Logger.getLogger(WeiboSyncSpark.class);
    public static final String SEP = "#";
    public static final byte[] R = "r".getBytes();
    public static final byte[] UID = "uid".getBytes();
    public static final byte[] TREND_UPDATE_DATE = "update_date".getBytes();
    public static final byte[] TREND_DATA = "data".getBytes();
    public static final int ES_SEARCH_BATCH = 1000;
    public static final int TREAND_MAX_VERSION = 30;
    public static final String P_CORES = "core";
    public static final String P_START_TIME = "start";
    public static final String P_END_TIME = "end";

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    public static final String PH_WB_CNT = "DS_BANYAN_WEIBO_CONTENT_V1";
    public static final String PH_WB_TREND_CNT = "DS_BANYAN_COMMON_TREND";
    public static final String PH_WB_USER = "DS_BANYAN_WEIBO_USER";

    public static final int MAX_TASK_COUNT = 50;

    public static final SentimentSourceType SENTIMENT_SOURCE_TYPE = SentimentSourceType.WEIBO;

    private QueryBuilder buildQueryBuilder(long start, long end) {

//        LOG.info("初始化参数开始");
//
//        QueryBuilder queryBuilder = CommonQueryBuilder.buildQueryBuilder(Arrays.asList("content"), Arrays.asList("content"), WeiboUtil.getkeyWords(), WeiboUtil.getFilterWords(),
//                FilterDoc.getInstance().getWeiboDocId(),
//                MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);
//
//        LOG.info("初始化参数结束");
////        return QueryBuilders.termQuery("id", "4076096184261816");
//        return queryBuilder;
        return buildQueryBuilder(WeiboUtil.getkeyWords(), start, end);
    }

    private QueryBuilder buildQueryBuilder(IWords keyWords, long start, long end) {

        LOG.info("初始化参数开始");
        LOG.info("keywrods:" + keyWords.getOriginWords());
        QueryBuilder queryBuilder = CommonQueryBuilder.buildQueryBuilder(Arrays.asList("content"), Arrays.asList("content"), keyWords, WeiboUtil.getFilterWords(), FilterDoc.getInstance().getWeiboDocId(), MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);

        LOG.info("初始化参数结束");
//        return QueryBuilders.termQuery("id", "4076096184261816");
        return queryBuilder;
    }

    public void run(long start, long end, int cores) {
        try {
            KeyWords keyWords = WeiboUtil.getkeyWords();

            Set<String> allKeys = keyWords.getAllKeys();

            LOG.info("all keys:" + allKeys);

            if (allKeys != null) {
                Map<String, String> tempKeyWordMap = new HashMap<String, String>();
                for (String key : allKeys) {
                    String originValue = keyWords.getOriginValue(key);
                    tempKeyWordMap.put(key, originValue);

                    if (tempKeyWordMap.size() >= MAX_TASK_COUNT) {
                        KeyWords tempKeywords = new KeyWords();
                        tempKeywords.buildKeyWords(tempKeyWordMap);
                        QueryBuilder queryBuilder = buildQueryBuilder(tempKeywords, start, end);
                        run(queryBuilder, start, end, cores);
                        tempKeyWordMap.clear();
                    }
                }

                if (tempKeyWordMap.size() > 0) {
                    KeyWords tempKeywords = new KeyWords();
                    tempKeywords.buildKeyWords(tempKeyWordMap);
                    QueryBuilder queryBuilder = buildQueryBuilder(tempKeywords, start, end);
                    run(queryBuilder, start, end, cores);
                }

            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    public void run(QueryBuilder queryBuilder, long start, long end, int cores) {
        try {
            long cnt = CommonReader.getInstance().getTotalHits(MigSyncConsts.ES_WEIBO_INDEX, MigSyncConsts.ES_WEIBO_CHILD_TYPE, queryBuilder);
            System.out.println("[Init Count] " + cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(queryBuilder.toString().replaceAll("\\n", ""));
        String appName = "dbamp-" + this.getClass().getSimpleName() + "-" + sdf.format(new Date(start)) + ":" + sdf.format(new Date(end));
        StrParams sparkConf = new StrParams();

        sparkConf.put("es.resource", MigSyncConsts.ES_WEIBO_INDEX + "/" + MigSyncConsts.ES_WEIBO_CHILD_TYPE);
        sparkConf.put("es.nodes", "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48");
        sparkConf.put("es.query", "{ \"fields\":[\"id\", \"retweet_id\"], \"query\": " + queryBuilder.toString() + " }");
        sparkConf.put("es.scroll.size", "1000");
//        System.setProperty("HADOOP_USER_NAME", "dota");
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);
        final Accumulator<Integer> selfWriteAcc = jsc.accumulator(0);
        final Accumulator<Integer> srcWriteAcc = jsc.accumulator(0);
        final Accumulator<Integer> rtWriteAcc = jsc.accumulator(0);
        final Accumulator<Integer> enterAcc = jsc.accumulator(0);

        try {
            JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, sparkConf);
            JavaRDD<String> rtMidRDD = esRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Map<String, Object>>>, String>() {
                @Override
                public Iterable<String> call(Iterator<Tuple2<String, Map<String, Object>>> iter) throws Exception {
                    HBaseReader hBaseReader = new HBaseReader() {
                        @Override
                        public String getTable() {
                            return PH_WB_CNT;
                        }
                    };
                    hBaseReader.setMaxCache(1000);

                    List<Pair<String, String>> mids = new ArrayList<>();
                    List<String> ret = new LinkedList<String>();
                    while (iter.hasNext()) {
                        enterAcc.add(1);
                        Tuple2<String, Map<String, Object>> tpl = iter.next();
                        System.out.println("[ES] " + tpl);
                        String mid = tpl._1();
                        Object o = tpl._2().get("retweet_id");
                        String rtMid = null;
                        // fuck es-spark
                        if (o instanceof List && ((List) o).isEmpty()) rtMid = (String) ((List) o).get(0);
                        else rtMid = String.valueOf(o);

                        if (valid(rtMid) && rtMid.startsWith("[")) rtMid = rtMid.substring(1, rtMid.length() - 1);
                        else if (!valid(rtMid)) rtMid = null;

                        mids.add(new Pair<String, String>(mid, rtMid));

                        // batch get uid
                        Get get = new Get(BanyanTypeUtil.wbcontentPK(mid).getBytes());
                        get.addColumn(R, UID);
                        List<Params> hbRes = hBaseReader.batchRead(get);
                        if (!CollectionUtil.isEmpty(hbRes)) {
                            flush(hbRes, mids, ret);
                        }
                    }

                    List<Params> hbRes = hBaseReader.flush();
                    if (!CollectionUtil.isEmpty(hbRes)) {
                        flush(hbRes, mids, ret);
                    }

                    return ret;
                }

                public List<String> flush(List<Params> hbRes, List<Pair<String, String>> mids, List<String> ret) {
                    int i = -1;
                    for (Params p : hbRes) {
                        i++;
                        try {
                            Pair<String, String> pair = mids.get(i);
                            String mid_ = pair.getFirst();
                            String uid_ = null;
                            if (valid(p)) {
                                uid_ = p.getString("uid");
                            }

                            String rtMid_ = pair.getSecond();
                            if (!valid(p) || !valid(uid_)) {
                                uid_ = getUidFromHBase(mid_);
                                if (!valid(uid_)) {
                                    if (!valid(rtMid_)) {
                                        uid_ = "null";
                                    } else {
                                        LOG.error("[empty params] weibo  : " + mids.get(i));
                                        continue;
                                    }
                                }
                            }

                            if (!valid(rtMid_)) rtMid_ = "";
                            ret.add(mid_ + SEP + uid_ + SEP + rtMid_);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    mids.clear();
                    return ret;
                }
            }).persist(StorageLevel.MEMORY_AND_DISK());

            // write itself to es
            rtMidRDD.flatMapToPair(new GroupMapFunction()).groupByKey(cores).mapPartitions(new MergeGroupIDFunc()).foreachPartition(new WeiboNUserWriterFunction(selfWriteAcc));

            // if it is a src_weibo, search those weibo who retweet it and write to es
            rtMidRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                //            new PairFlatMapFunction<Iterator<String>, String, String>() {
                @Override
//                                public Iterable<Tuple2<String, String>> call(Iterator<String> iter) throws Exception {
                public Iterable<String> call(Iterator<String> iter) throws Exception {
                    List<String> rtMids = new LinkedList<String>();
                    List<String> paire = new LinkedList<String>();

                    while (iter.hasNext()) {
                        try {
                            String mergeID = iter.next();
                            System.out.println("[SRC] " + mergeID);
                            if (!mergeID.endsWith(SEP)) continue;
                            String[] arr = mergeID.split(SEP);
                            String mid = arr[0];
                            rtMids.add(mid);
                            if (rtMids.size() >= ES_SEARCH_BATCH) {
                                // esSearcher get mid & uid
                                List<String> tmp = queryMidNUid(rtMids);
                                if (!tmp.isEmpty()) paire.addAll(tmp);
                                rtMids.clear();
                            }
                        } catch (Exception e) {
                            LOG.error("[SRC] " + e.getMessage(), e);
                        }
                    }

                    if (rtMids.size() > 0) {
                        // esSearcher get mid & uid
                        List<String> tmp = queryMidNUid(rtMids);
                        if (!tmp.isEmpty()) paire.addAll(tmp);
                        rtMids.clear();
                    }

                    return paire;
                }
            }).flatMapToPair(new GroupMapFunction()).groupByKey(cores).mapPartitions(new MergeGroupIDFunc()).foreachPartition(new WeiboNUserWriterFunction(srcWriteAcc));

            // if it has a retweet weibo
            rtMidRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                @Override
                public Iterable<String> call(Iterator<String> iter) throws Exception {
                    List<String> rtMids = new LinkedList<String>();
                    while (iter.hasNext()) {
                        String mergeID = iter.next();
                        try {
                            System.out.println("[LW] " + mergeID);
                            if (mergeID.endsWith(SEP)) continue;
                            String[] arr = mergeID.split(SEP);
                            String rtMid = arr[2];
                            if (valid(rtMid)) {
                                rtMids.add(rtMid);
                            }
                        } catch (Exception ignore) {
                            LOG.error("[LW] " + mergeID);
                        }

                    }
                    return rtMids;
                }
            }).flatMapToPair(new GroupMapFunction()).groupByKey(cores).mapPartitions(new MergeGroupIDFunc()).foreachPartition(new WeiboNUserWriterFunction1(rtWriteAcc));

            System.out.println("[ACCUMULATOR] selfWriteAcc=" + selfWriteAcc.value());
            System.out.println("[ACCUMULATOR] srcWriteAcc=" + srcWriteAcc.value());
            System.out.println("[ACCUMULATOR] rtWriteAcc=" + rtWriteAcc.value());
            System.out.println("[ACCUMULATOR] enterAcc=" + enterAcc.value());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jsc.stop();
            jsc.close();
        }
    }


    /**
     * **************************************
     * Tool functions
     */
    public static List<String> queryMidNUid(List<String> retweetIds) {
        List<String> ret = new LinkedList<>();
        if (!CollectionUtil.isEmpty(retweetIds)) try {
            CommonReader esReader = CommonReader.getInstance();
            TermsQueryBuilder qb = QueryBuilders.termsQuery("retweet_id", retweetIds);
            EsReaderResult res = esReader.scrollWithoutSource(MigSyncConsts.ES_WEIBO_INDEX, MigSyncConsts.ES_WEIBO_CHILD_TYPE, 0, ES_SEARCH_BATCH, qb);
            String scrollID = res.getScrollId();
            System.out.println(scrollID + " : " + qb);
            while (res.getDatas() != null && res.getDatas().length > 0) {
                for (SearchHit hit : res.getDatas()) {
                    String mid = hit.getId();
                    String uid = null;
                    try {
                        SearchHitField searchHitField = hit.getFields().get("_parent");
                        if (searchHitField != null) uid = searchHitField.getValue();
                    } catch (Exception ignore) {
                    }
                    if (valid(uid)) ret.add(mid + SEP + uid + SEP);
                }
                res = esReader.search(scrollID);
            }
            System.out.println("[queryMidNUid] " + ret.size() + " => " + ret);
        } catch (Exception e) {
            LOG.error("[queryMidNUid] " + e.getMessage(), e);
        }
        return ret;
    }

    public static String getUidFromHBase(String mid) {
        if (StringUtil.isNullOrEmpty(mid)) return null;
        String pk = BanyanTypeUtil.wbcontentPK(mid);
        int retry = 1;
        Exception e = null;
        RFieldGetter getter = new RFieldGetter(PH_WB_CNT);
        Get get = new Get(pk.getBytes());
        get.addColumn(R, UID);
        while (retry-- > 0) {
            try {
                String uid = getter.singleGet(get, "uid");
                if (valid(uid)) return uid;
            } catch (Exception e1) {
                e = e1;
            }
        }
        if (e != null) e.printStackTrace();
        return null;
    }

    public static Params getFromHBase(String table, String id) {
        if (StringUtil.isNullOrEmpty(id) || "null".equals(id)) return null;
        String pk = BanyanTypeUtil.sub3PK(id);
        int retry = 1;
        Exception e = null;
        RFieldGetter getter = new RFieldGetter(table);
        while (retry-- > 0) {
            try {
                Params u;
                if (table.equals(PH_WB_USER)) u = getter.get(buildUserGet(pk));
                else u = getter.get(pk);
                return u;
            } catch (Exception e1) {
                e = e1;
            }
        }
        if (e != null) e.printStackTrace();
        return null;
    }

    public static Get buildUserGet(String uid) {
        if (!valid(uid)) return null;
        String pk = BanyanTypeUtil.wbuserPK(uid);
        Get get = new Get(pk.getBytes());
        get.addColumn(R, "uid".getBytes());
        get.addColumn(R, "name".getBytes());
        get.addColumn(R, "gender".getBytes());
        get.addColumn(R, "constellation".getBytes());
        get.addColumn(R, "fans_level".getBytes());
        get.addColumn(R, "verified_type".getBytes());
        get.addColumn(R, "sources".getBytes());
        get.addColumn(R, "city".getBytes());
        get.addColumn(R, "birthyear".getBytes());
        get.addColumn(R, "birthdate".getBytes());
        get.addColumn(R, "user_type".getBytes());
        get.addColumn(R, "vtype".getBytes());
        get.addColumn(R, "fans_cnt".getBytes());
        get.addColumn(R, "follow_cnt".getBytes());
        get.addColumn(R, "wb_cnt".getBytes());
        get.addColumn(R, "fav_cnt".getBytes());
        get.addColumn(R, "province_cnt".getBytes());
        get.addColumn(R, "city_level".getBytes());
        get.addColumn(R, "company".getBytes());
        get.addColumn(R, "school".getBytes());
        get.addColumn(R, "desc".getBytes());
        get.addColumn(R, "publish_date".getBytes());
        get.addColumn(R, "update_date".getBytes());
        get.addColumn(R, "activeness".getBytes());

        return get;
    }

    /**
     * **************************************
     * batch read weibo & user and write them to ES
     */
    public static class WeiboNUserWriterFunction implements VoidFunction<Iterator<String>> {
        protected Accumulator<Integer> accumulator = null;

        public WeiboNUserWriterFunction() {
        }

        public WeiboNUserWriterFunction(Accumulator<Integer> accumulator) {
            this.accumulator = accumulator;
        }

        public HashMap<String, Params> flushUser(CommonWriter userEsWriter, List<Params> userRes, ArrayList<String> uidCache) {
            int i = -1;
            HashMap<String, Params> users = new HashMap<>();
            for (Params p : userRes) {
                i++;
                if (!valid(p)) {
                    p = getFromHBase(PH_WB_USER, uidCache.get(i));
                    if (!valid(p)) {
                        LOG.error("[empty user] " + uidCache.get(i));
                        continue;
                    }
                }
                String uid_ = p.getString("uid");
                if (valid(uid_)) {
                    users.put(uid_, p);
                    // translate doc to YZDoc and write ES
                    YZDoc yzDoc = new Doc2UserWrapper(p).objWrapper();
                    try {
                        userEsWriter.addData(yzDoc);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return users;
        }

        public void flushContent(CommonWriter cntEsWriter, List<Params> cntRes, ArrayList<String> midCache, HashMap<String, Params> userRes) {


            int i = -1;
            for (Params p : cntRes) {
                i++;
                if (!valid(p)) {
                    p = getFromHBase(PH_WB_CNT, midCache.get(i));
                    if (!valid(p)) {
                        LOG.error("[empty weibo] " + midCache.get(i));
                        continue;
                    }
                }
                String uid_ = p.getString("uid");
                if (valid(uid_)) {
                    Params user = userRes.get(uid_);
                    int isReal = 1;
                    if (valid(user)) {
                        BanyanTypeUtil.safePut(p, "username", user.getString("name"));
                        isReal = isReal(user.getString("user_type"));
                    }
                    p.put("is_real", isReal);
                }
            }

            //计算趋势分布,并将其写入到cntRes
            getTrendDist(cntRes, midCache);

            for (Params p : cntRes) {
                if (p == null) {
                    continue;
                }
                //System.out.println("[PARAMS] " + p.toJson());
                String uid_ = p.getString("uid");
                // translate doc to YZDoc and write ES
                YZDoc yzDoc = new WbCnt2MigESDocMapper(p).yzDoc();
//                try {
//                    System.out.println("[YZDOC] " + yzDoc.toJson());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }

                //分析操作
                WeiboUtil.setMid(yzDoc);//设置mid
                WeiboUtil.setWeiboSelfContent(yzDoc);
                WeiboUtil.setWeiboMsgType(yzDoc);
                WeiboUtil.sentimentAnaly(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SENTIMENT_SOURCE_TYPE);//根据关键词分析本体的情感;对每一个品牌,活动
                WeiboUtil.extractHighFreqyWord(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD);
                WeiboUtil.removeTopicAnalySentiment(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SENTIMENT_SOURCE_TYPE);
                if (YzDocUtil.isExistAndNotEmpty(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) && YzDocUtil.isExistAndNotEmpty(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT)) {
                    LOG.info("[analy retweet sentiment] " + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) + "#" + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT));
                    WeiboUtil.removeTopicAnalySentiment(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT, MigSyncConsts.ES_FIELD_RETWEET_SENTIMENT, SENTIMENT_SOURCE_TYPE);
                } else {
                    LOG.info("[not exist retweet id] " + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) + "#" + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT));
                }
                try {
                    if (valid(uid_)) cntEsWriter.addData(yzDoc, uid_);
                    else LOG.error("[no uid] " + p.getString("id") + "#" + p.getString("uid"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (accumulator != null) accumulator.add(1);
            }

        }

        @Override
        public void call(Iterator<String> iter) throws Exception {
            HBaseReader cntReader = new HBaseReader() {
                @Override
                public String getTable() {
                    return "DS_BANYAN_WEIBO_CONTENT_V1";
                }
            };

            HBaseReader userReader = new HBaseReader() {
                @Override
                public String getTable() {
                    return "DS_BANYAN_WEIBO_USER";
                }
            };

            MigEsWeiboCntWriter cntEsWriter = MigEsWeiboCntWriter.getInstance();
            MigEsWeiboUserWriter userEsWriter = MigEsWeiboUserWriter.getInstance();

            ArrayList<String> midCache = new ArrayList<>();
            ArrayList<String> uidCache = new ArrayList<>();

            while (iter.hasNext()) {
                String mergeID = iter.next();
                System.out.println("[WRITE]" + mergeID);//3982003109823336
                String[] arr = mergeID.split(SEP);
                String mid = arr[0];
                String uid = arr[1];
                // read from hbase
                HashMap<String, Params> users = new HashMap<>();
                List<Params> userRes = null;
                if (valid(uid) && !uidCache.contains(uid)) {
                    uidCache.add(uid);
                    userRes = userReader.batchRead(BanyanTypeUtil.wbuserPK(uid));
                }

                midCache.add(mid);
                List<Params> cntRes = cntReader.batchRead(BanyanTypeUtil.wbcontentPK(mid));
                if (!CollectionUtil.isEmpty(cntRes)) {
                    if (CollectionUtil.isEmpty(userRes)) {
                        userRes = userReader.flush();
                    }

                    if (!CollectionUtil.isEmpty(userRes)) {
                        try {
                            users = flushUser(userEsWriter, userRes, uidCache);
                        } finally {
                            uidCache.clear();
                        }
                    }
                    try {
                        flushContent(cntEsWriter, cntRes, midCache, users);
                    } finally {
                        midCache.clear();
                    }
                }
            }

            try {
                List<Params> userRes = userReader.flush();
                HashMap<String, Params> users = new HashMap<>();
                if (!CollectionUtil.isEmpty(userRes)) {
                    try {
                        users = flushUser(userEsWriter, userRes, uidCache);
                    } finally {
                        uidCache.clear();
                    }
                }
                List<Params> cntRes = cntReader.flush();
                if (!CollectionUtil.isEmpty(cntRes)) {
                    try {
                        flushContent(cntEsWriter, cntRes, midCache, users);
                    } finally {
                        midCache.clear();
                    }
                }

                cntEsWriter.flush();
                userEsWriter.flush();
            } catch (Exception e) {
                LOG.error(e.getMessage() + " : " + e.getStackTrace()[0]);
            } finally {
            }
        }
    }

    /**
     * 计算转拼赞的趋势分布
     *
     * @param cntRes
     * @param midCache
     */
    public static void getTrendDist(List<Params> cntRes, ArrayList<String> midCache) {

        //System.out.println("chenchuanyu:" + midCache);

        TrendHBaseReader trendHBaseReader = new TrendHBaseReader();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        //获取点赞分布,评论分布,转发分布
        for (int i = 0; i < cntRes.size(); i++) {
            try {
                String mid = midCache.get(i);
                Params cntReParams = cntRes.get(i);
                System.out.println("开始计算mid:" + mid + "的趋势分布!");
                //System.out.println("params:" + cntReParams.toJson());
                Params params = trendHBaseReader.readTrend(HBaseUtils.wbTrendPK(mid), TREAND_MAX_VERSION);

                if (params != null && cntReParams != null && params.containsKey("data")) {
                    Map<Long, byte[]> datas = (Map<Long, byte[]>) params.get("data");
                    //日期到 时间戳 到 趋势值, 为了合并每天最晚时间的值。
                    Map<String, Pair<Long, String>> dateToTimestampToTrend = new TreeMap<>();
                    for (Map.Entry<Long, byte[]> entry : datas.entrySet()) {
                        try {
                            Long timestamp = entry.getKey();
                            String date = sdf.format(new Date(timestamp));
                            String trendValue = entry.getValue() != null ? new String(entry.getValue()) : "";
                            if (StringUtils.isNotEmpty(trendValue) && StringUtils.isNotEmpty(date)) {
                                if (!dateToTimestampToTrend.containsKey(date) || dateToTimestampToTrend.get(date).getFirst() < timestamp) {
                                    dateToTimestampToTrend.put(date, new Pair<Long, String>(timestamp, trendValue));
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    int prefixAttitudeTotal = 0;
                    int prefixRepostTotal = 0;
                    int prefixCommentTotal = 0;

                    long maxAttitude = -1;
                    long maxRepost = -1;
                    long maxComment = -1;

                    List<Map<String, Object>> listReposts = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> listComments = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> listAttitudes = new ArrayList<Map<String, Object>>();
                    //生成对应的点赞分布,评论分布、转发分布
                    for (Map.Entry<String, Pair<Long, String>> entry : dateToTimestampToTrend.entrySet()) {
                        String updateDate = entry.getKey();
                        String trendValue = entry.getValue().getSecond();
                        Triple<Integer, Integer, Integer> trendValue1 = WeiboUtil.getTrendValue(trendValue);
                        Integer repostValue = trendValue1.getFirst();
                        Integer commentValue = trendValue1.getSecond();
                        Integer attitudeValue = trendValue1.getThird();

                        if (attitudeValue != null && maxAttitude < attitudeValue) {
                            maxAttitude = attitudeValue;
                        }
                        if (commentValue != null && maxComment < commentValue) {
                            maxComment = commentValue;
                        }
                        if (repostValue != null && maxRepost < repostValue) {
                            maxRepost = repostValue;
                        }

                        NestedStructureUtil.parseTrend(listReposts, updateDate, repostValue - prefixRepostTotal);
                        NestedStructureUtil.parseTrend(listComments, updateDate, commentValue - prefixCommentTotal);
                        NestedStructureUtil.parseTrend(listAttitudes, updateDate, attitudeValue - prefixAttitudeTotal);
                        prefixAttitudeTotal = attitudeValue;
                        prefixCommentTotal = commentValue;
                        prefixRepostTotal = repostValue;
                    }

                    //插入到对应的params
                    cntReParams.put(ES_ALL_WEIBO_WEIBO_REPOSTS, listReposts);
                    cntReParams.put(ES_ALL_WEIBO_WEIBO_COMMENTS, listComments);
                    cntReParams.put(ES_ALL_WEIBO_WEIBO_ATTITUDES, listAttitudes);

                    if (maxRepost >= 0) {
                        cntReParams.put(ES_ALL_WEIBO_WEIBO_REPOST_CNT, maxRepost);
                    }
                    if (maxComment >= 0) {
                        cntReParams.put(ES_ALL_WEIBO_WEIBO_COMMENT_CNT, maxComment);
                    }
                    if (maxAttitude >= 0) {
                        cntReParams.put(ES_ALL_WEIBO_WEIBO_ATTITUDE_CNT, maxAttitude);
                    }

                    //System.out.println("news params:" + cntReParams.toJson());
                    System.out.printf("[TREND]mid:%s,repost:%s,comment:%s,attitude:%s\n", mid, listReposts, listComments, listAttitudes);
                } else {
                    if (cntReParams == null) {
                        System.out.println("cntReParams!mid:" + mid);
                    }
//                    System.out.println(params == null ? "param等于空!mid:" + mid : params.toJson());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * **************************************
     * another batch read weibo & user and write them to ES
     */
    public static class WeiboNUserWriterFunction1 implements VoidFunction<Iterator<String>> {
        protected Accumulator<Integer> accumulator = null;

        public WeiboNUserWriterFunction1(Accumulator<Integer> accumulator) {
            this.accumulator = accumulator;
        }

        public WeiboNUserWriterFunction1() {
        }

        public void flush(CommonWriter cntEsWriter, List<Params> cntRes, ArrayList<String> midCache, CommonWriter userEsWriter, List<Params> userRes, ArrayList<String> uidCache) {
            int i = -1;
            HashMap<String, Params> users = new HashMap<>();
            for (Params p : userRes) {
                i++;
                if (!valid(p)) {
                    p = getFromHBase(PH_WB_USER, uidCache.get(i));
                    if (!valid(p)) {
                        LOG.error("[empty user1] " + uidCache.get(i));
                        continue;
                    }
                }
                // translate doc to YZDoc and write ES
                String uid_ = p.getString("uid");
                if (valid(uid_)) {
                    users.put(uid_, p);
                    YZDoc yzDoc = new Doc2UserWrapper(p).objWrapper();
                    try {
                        userEsWriter.addData(yzDoc);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }


            i = -1;
            for (Params p : cntRes) {
                i++;
                if (!valid(p)) {
                    p = getFromHBase(PH_WB_CNT, midCache.get(i));
                    if (!valid(p)) {
                        LOG.error("[empty weibo1] " + midCache.get(i));
                        continue;
                    }
                }
                // translate doc to YZDoc and write ES
                String uid_ = p.getString("uid");
                if (valid(uid_)) {
                    Params user = users.get(uid_);
                    int isReal = 1;
                    if (valid(user)) {
                        BanyanTypeUtil.safePut(p, "username", user.getString("name"));
                        isReal = isReal(user.getString("user_type"));
                    }
                    p.put("is_real", isReal);
                }
            }

            //计算趋势分布,并将其写入到cntRes
            getTrendDist(cntRes, midCache);

            for (Params p : cntRes) {
                if (p == null) {
                    continue;
                }
                //System.out.println("[PARAMS] " + p.toJson());
                String uid_ = p.getString("uid");
                YZDoc yzDoc = new WbCnt2MigESDocMapper(p).yzDoc();

//                try {
//                    System.out.println("[YZDOC] " + yzDoc.toJson());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }

                //分析操作
                WeiboUtil.setMid(yzDoc);
                WeiboUtil.setWeiboSelfContent(yzDoc);
                WeiboUtil.setWeiboMsgType(yzDoc);
                WeiboUtil.sentimentAnaly(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SENTIMENT_SOURCE_TYPE);//根据关键词分析本体的情感;对每一个品牌,活动
                WeiboUtil.extractHighFreqyWord(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD);
                WeiboUtil.removeTopicAnalySentiment(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SENTIMENT_SOURCE_TYPE);
                if (YzDocUtil.isExistAndNotEmpty(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) && YzDocUtil.isExistAndNotEmpty(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT)) {
                    LOG.info("[analy retweet sentiment] " + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) + "#" + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT));
                    WeiboUtil.removeTopicAnalySentiment(yzDoc, MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT, MigSyncConsts.ES_FIELD_SENTIMENT, SENTIMENT_SOURCE_TYPE);
                } else {
                    LOG.info("[not exist retweet id] " + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID) + "#" + yzDoc.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT));
                }

                try {
                    if (valid(uid_)) cntEsWriter.addData(yzDoc, uid_);
                    else LOG.error("[no uid1] " + p.getString("id") + "#" + p.getString("uid"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (accumulator != null) accumulator.add(1);
            }

        }

        @Override
        public void call(Iterator<String> iter) throws Exception {
            HBaseReader cntReader = new HBaseReader() {
                @Override
                public String getTable() {
                    return PH_WB_CNT;
                }
            };

            HBaseReader userReader = new HBaseReader() {
                @Override
                public String getTable() {
                    return PH_WB_USER;
                }
            };

            MigEsWeiboCntWriter cntEsWriter = MigEsWeiboCntWriter.getInstance();
            MigEsWeiboUserWriter userEsWriter = MigEsWeiboUserWriter.getInstance();
            ArrayList<String> midCache = new ArrayList<>();
            ArrayList<String> uidCache = new ArrayList<>();

            while (iter.hasNext()) {
                String mid = iter.next();
                // read from hbase
                midCache.add(mid);
                List<Params> cntRes = cntReader.batchRead(BanyanTypeUtil.wbcontentPK(mid));
                if (!CollectionUtil.isEmpty(cntRes)) {
                    List<Params> userRes = null;
                    for (Params p : cntRes) {
                        String uid = null;
                        if (valid(p)) uid = p.getString("uid");
                        if (valid(uid) && !uidCache.contains(uid)) {
                            uidCache.add(uid);
                            userRes = userReader.batchRead(BanyanTypeUtil.wbuserPK("" + uid));
                        }
                    }
                    if (CollectionUtil.isEmpty(userRes)) {
                        userRes = userReader.flush();
                    }

                    try {
                        flush(cntEsWriter, cntRes, midCache, userEsWriter, userRes, uidCache);
                    } finally {
                        midCache.clear();
                        uidCache.clear();
                    }
                }
            }

            try {
                List<Params> cntRes = cntReader.flush();
                if (!CollectionUtil.isEmpty(cntRes)) {
                    try {
                        List<Params> userRes = null;
                        for (Params p : cntRes) {
                            String uid = null;
                            if (valid(p)) uid = p.getString("uid");
                            if (valid(uid) && !uidCache.contains(uid)) {
                                uidCache.add(uid);
                                userRes = userReader.batchRead(BanyanTypeUtil.wbuserPK("" + uid));
                            }
                        }
                        if (CollectionUtil.isEmpty(userRes)) {
                            userRes = userReader.flush();
                        }

                        flush(cntEsWriter, cntRes, midCache, userEsWriter, userRes, uidCache);
                    } finally {
                        midCache.clear();
                        uidCache.clear();
                    }
                }

                cntEsWriter.flush();
//                cntEsWriter.close();
                userEsWriter.flush();
//                userEsWriter.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            } finally {

            }
        }
    }

    public static class MergeGroupIDFunc implements FlatMapFunction<Iterator<Tuple2<String, Iterable<String>>>, String> {
        @Override
        public Iterable<String> call(Iterator<Tuple2<String, Iterable<String>>> tplIter) throws Exception {
            List<String> mids = new LinkedList<String>();
            while (tplIter.hasNext()) {
                for (String mid : tplIter.next()._2()) {
                    mids.add(mid);
                }
            }
            return mids;
        }
    }

    public static class GroupMapFunction implements PairFlatMapFunction<String, String, String> {
        @Override
        public Iterable<Tuple2<String, String>> call(String id) throws Exception {
            String mid = id.split(SEP)[0];
            return Arrays.asList(new Tuple2<>(Md5Util.md5(mid).substring(0, 3), id));
        }
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

    /**
     * *****************
     * 没用的main
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        int cores = 80;
        if (args.length > 0) cores = Integer.valueOf(args[0]);
        WeiboSyncSpark runner = new WeiboSyncSpark();
//        runner.cores = cores;
        System.out.println(WeiboSyncSpark.getUidFromHBase("4071140555412444"));

        System.out.println("[PROGRAM] Program exited.");
    }
}
