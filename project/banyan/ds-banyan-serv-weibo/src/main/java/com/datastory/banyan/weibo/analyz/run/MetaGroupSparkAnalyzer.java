package com.datastory.banyan.weibo.analyz.run;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datatub.scavenger.base.MgroupConsts;
import com.datatub.scavenger.util.MathUtil;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Preconditions;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * com.datastory.banyan.weibo.analyz.run.MetaGroupSparkAnalyzer
 *
 * 这tm大概是我写过最复杂的spark dag了。。。
 * 然而好像白写了，mr版的调内存能跑。
 * @author lhfcws
 * @since 2016/12/30
 */
@Deprecated
public class MetaGroupSparkAnalyzer implements Serializable {
    private static final String[] FANS_RANGE = {"1000", "500000"}; // 讲道理，这个范围应该是算法组提供接口的。
    public static final String SELF_PREFIX = "_";
    public static final byte[] R = "r".getBytes();
    public static final byte[] UID = "uid".getBytes();
    public static final byte[] TAG_DIST = "tag_dist".getBytes();
    public static final byte[] FANS_CNT = "fans_cnt".getBytes();
    //    public static final byte[] FANS_CNT = "follow_count".getBytes();
    public static final byte[] META_GROUP = "meta_group".getBytes();
    public static final byte[] F = "f".getBytes();
    public static final String table = Tables.table(Tables.PH_WBUSER_TBL);

    public static int alpha = MgroupConsts.DEFAULT_ALPHA; // alpha - 平滑函数，用以平滑那些tag在用户微博中没有出现的情况
    public static int minOcc = MgroupConsts.DEFALUT_MIN_OCC; // 最少出现的 tag 次
    public static double threshold = MgroupConsts.DEFAULT_THRESHOLD; // 权重小于0.01的tag会被过滤掉

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.addFamily(F);
        scan.addColumn(R, UID);
        scan.addColumn(R, TAG_DIST);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.GREATER_OR_EQUAL, FANS_RANGE[0].getBytes());
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.LESS_OR_EQUAL, FANS_RANGE[1].getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        scan.setFilter(filterList);

        return scan;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Scan scan = buildScan();
        StrParams sparkConf = new StrParams();
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.weibo.analyz.run.MetaGroupSparkAnalyzer.MGKryoRegister");
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, "MetaGroupSparkAnalyzer", "512", sparkConf);
        final Accumulator<Integer> userAccumulator = jsc.accumulator(0);

        final Configuration conf = new Configuration(RhinoETLConfig.getInstance());
        conf.set(TableInputFormat.INPUT_TABLE, table);
        conf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        try {
            // 同tag 的 TagDistEntry merge 函数
            final Function2<TagDistEntry, TagDistEntry, TagDistEntry> mergeFunc = new Function2<TagDistEntry, TagDistEntry, TagDistEntry>() {
                @Override
                public TagDistEntry call(TagDistEntry tagDistEntry, TagDistEntry tagDistEntry2) throws Exception {
                    if (tagDistEntry.isSelf()) {
                        tagDistEntry.merge(tagDistEntry2);
                        return tagDistEntry;
                    } else {
                        tagDistEntry2.merge(tagDistEntry);
                        return tagDistEntry2;
                    }
                }
            };


            // Begin spark
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            hBaseRDD
                    // 计算 tagDist， 按照tag分发出去
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, TagDistEntry>() {
                        @Override
                        public Iterable<Tuple2<String, TagDistEntry>> call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                            LinkedList<Tuple2<String, TagDistEntry>> list = new LinkedList<>();
                            Result result = v1._2();
                            String tag_dist = HBaseUtils.getValue(result, R, TAG_DIST);
                            String uid = HBaseUtils.getValue(result, R, UID);
                            if (StringUtil.isNullOrEmpty(tag_dist))
                                return list;

                            // 解析self tagDist
                            try {
                                TagDistSer self = new TagDistSer().parse(tag_dist);
                                List<TagDistEntry> entries = TagDistEntry.parseTagDist(uid, uid, self);
                                if (CollectionUtil.isEmpty(entries))
                                    return list;

                                for (TagDistEntry entry : entries) {
                                    list.add(new Tuple2<>(uid + "$" + entry.getTag(), entry));
                                }
                                Map<String, String> fansMap = HBaseUtils.getValues(result, F);
                                if (fansMap != null && !fansMap.isEmpty())
                                    for (String fansId : fansMap.keySet()) {
                                        for (TagDistEntry entry : entries) {
                                            TagDistEntry te = entry.clone();
                                            te.setUid(fansId);
                                            list.add(new Tuple2<>(fansId + "$" + entry.getTag(), te));
                                        }
                                    }
                                userAccumulator.add(1);
                            } catch (Exception e) {
                            }
                            return list;
                        }
                    })
                    // 本地 combine一次
                    .combineByKey(new Function<TagDistEntry, TagDistEntry>() {
                        @Override
                        public TagDistEntry call(TagDistEntry tagDistEntry) throws Exception {
                            return tagDistEntry;
                        }
                    }, mergeFunc, mergeFunc)
                    // shuffle， 远程 reduce
                    .reduceByKey(mergeFunc)
                    // 过滤小于阈值的tag
                    .filter(new Function<Tuple2<String, TagDistEntry>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<String, TagDistEntry> tpl) throws Exception {
                            TagDistEntry te = tpl._2();
                            return te.getTagCount() > minOcc && te.getTagWeight() > threshold;
                        }
                    })
                    // 将key从 uid&tag 变为  uid
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<String, TagDistEntry>, String, TagDistEntry>() {
                        @Override
                        public Iterable<Tuple2<String, TagDistEntry>> call(Tuple2<String, TagDistEntry> _tpl) throws Exception {
                            String uid = _tpl._1().split(StringUtil.STR_DELIMIT_2ND)[0];
                            Tuple2<String, TagDistEntry> tpl = new Tuple2<String, TagDistEntry>(uid, _tpl._2());
                            return Arrays.asList(tpl);
                        }
                    })
                    // 本地按用户合并
                    .combineByKey(new Function<TagDistEntry, TagDistSer>() {
                        @Override
                        public TagDistSer call(TagDistEntry tagDistEntry) throws Exception {
                            return new TagDistSer(tagDistEntry);
                        }
                    }, new Function2<TagDistSer, TagDistEntry, TagDistSer>() {
                        @Override
                        public TagDistSer call(TagDistSer tagDistSer, TagDistEntry tagDistEntry) throws Exception {
                            return tagDistSer.addEntry(tagDistEntry);
                        }
                    }, new Function2<TagDistSer, TagDistSer, TagDistSer>() {
                        @Override
                        public TagDistSer call(TagDistSer tagDistSer, TagDistSer tagDistSer2) throws Exception {
                            return tagDistSer.merge(tagDistSer2);
                        }
                    })
                    // 远程按用户合并
                    .reduceByKey(new Function2<TagDistSer, TagDistSer, TagDistSer>() {
                        @Override
                        public TagDistSer call(TagDistSer tagDistSer, TagDistSer tagDistSer2) throws Exception {
                            return tagDistSer.merge(tagDistSer2);
                        }
                    })
                    // 批量写出到hbase
                    .foreachPartition(new VoidFunction<Iterator<Tuple2<String, TagDistSer>>>() {
                        @Override
                        public void call(Iterator<Tuple2<String, TagDistSer>> iter) throws Exception {
                            RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_WBUSER_TBL));
                            while (iter.hasNext()) {
                                Tuple2<String, TagDistSer> tpl = iter.next();
                                TagDistSer t = tpl._2();
                                String metagroup = t.metagroupString();

                                if (!StringUtil.isNullOrEmpty(metagroup)) {
                                    String pk = BanyanTypeUtil.wbuserPK(tpl._1());
                                    Params p = new Params("pk", pk);
                                    p.put("meta_group", metagroup);

                                    putter.batchWrite(p);
                                }
                                putter.flush();
                            }
                        }
                    })
            ;

        } finally {
            jsc.stop();
            jsc.close();
            System.out.println("[STAT] Total affected user: " + userAccumulator.value());
        }
    }

    // ===============================================

    public static class MGKryoRegister implements KryoRegistrator {
        public static Class[] KRYO_CLASSES = {
                String.class, TagDistEntry.class, TagDistSer.class
        };

        @Override
        public void registerClasses(Kryo kryo) {
            for (Class<?> klass : KRYO_CLASSES) {
                kryo.register(klass);
            }
        }
    }

    public static class TagDistSer implements Serializable {
        private int selfAcc = 0;
        private int otherAcc = 0;
        private double confidence = 0.0D;
        private double sum = 0;
        private Map<String, Integer> tagCount = new HashMap<>();
        private Map<String, Double> tagWeight = new HashMap<>();

        public TagDistSer() {
        }

        public TagDistSer(TagDistEntry e) {
            this.confidence = e.getConfidence();
            this.sum = e.sum;
            this.tagCount.put(e.getTag(), e.getTagCount());
            this.tagWeight.put(e.getTag(), e.getTagWeight());
            selfAcc = e.isSelf() ? 1 : 0;
        }

        public TagDistSer merge(TagDistSer t) {
            this.selfAcc += t.selfAcc;
            this.otherAcc += t.otherAcc;
            this.tagCount.putAll(t.getTagCount());
            this.tagWeight.putAll(t.getTagWeight());
            return this;
        }

        public TagDistSer parse(String tagDist) throws Exception {
            Preconditions.checkArgument(StringUtils.isNotEmpty(tagDist));
            Preconditions.checkArgument(MgroupConsts.PATTERN_TAG_DIST.matcher(tagDist).find());
            String[] dists = tagDist.split("\\|");
            String[] tweetsCount = dists[0].split("\\$");
            double tweets_covered_count = Double.valueOf(tweetsCount[0]);
            int tweets_total_count = Integer.valueOf(tweetsCount[1]);
            this.confidence = tweets_covered_count / (double) tweets_total_count;

            for (int i = 1; i < dists.length; ++i) {
                String[] tagWeight = dists[i].split("\\$");
                this.tagCount.put(tagWeight[0], Integer.valueOf(tagWeight[1]));
            }

            this.sum = alpha * tagCount.size() + MathUtil.sum(tagCount.values());

            return this;
        }

        public TagDistSer addEntry(TagDistEntry entry) {
            if (selfAcc == 0 && entry.isSelf())
                selfAcc = 1;
            else
                otherAcc ++;
            this.tagCount.put(entry.getTag(), entry.getTagCount());
            this.tagWeight.put(entry.getTag(), entry.getTagWeight());
            return this;
        }

        public double getSum() {
            return sum;
        }

        public double getConfidence() {
            return confidence;
        }

        public Map<String, Integer> getTagCount() {
            return tagCount;
        }

        public Map<String, Double> getTagWeight() {
            return tagWeight;
        }

        public int getSelfAcc() {
            return selfAcc;
        }

        public int getOtherAcc() {
            return otherAcc;
        }

        public String metagroupString() {
            // 输出格式
            StringBuilder sb = new StringBuilder();
            sb.append(selfAcc).append(StringUtil.DELIMIT_2ND).append(otherAcc);
            for (Map.Entry<String, Double> e : tagWeight.entrySet()) {
                sb.append(StringUtil.DELIMIT_1ST).append(e.getKey())
                        .append(StringUtil.DELIMIT_2ND)
                        .append(tagCount.get(e.getKey()))
                        .append(StringUtil.DELIMIT_2ND)
                        .append(String.format("%.3f", e.getValue()));
            }

            return sb.toString();
        }
    }

    public static class TagDistEntry implements Serializable, Cloneable {
        private String uid;
        private String followId;
        private double sum = 0;
        private double confidence = 0.0D;
        private String tag;
        private int tagCount;
        private double tagWeight = 0;

        public TagDistEntry(String uid, String followId, double sum, double confidence, String tag, int tagCount) {
            this.uid = uid;
            this.followId = followId;
            this.sum = sum;
            this.confidence = confidence;
            this.tag = tag;
            this.tagCount = tagCount;
        }

        public TagDistEntry(String uid, String followId, double sum, double confidence, Map.Entry<String, Integer> e) {
            this.uid = uid;
            this.followId = followId;
            this.sum = sum;
            this.confidence = confidence;
            this.tag = e.getKey();
            this.tagCount = e.getValue();
        }

        public TagDistEntry merge(TagDistEntry te) {
            this.tagWeight += te.tagWeight;
            this.tagCount += te.tagCount;
            return this;
        }

        public double getTagWeight() {
            return tagWeight;
        }

        public void setTagWeight(double tagWeight) {
            this.tagWeight = tagWeight;
        }

        public void addTagWeight(double tagWeight) {
            this.tagWeight += tagWeight;
        }

        public boolean isSelf() {
            return uid.equals(followId);
        }

        public String getFollowId() {
            return followId;
        }

        public void setFollowId(String followId) {
            this.followId = followId;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public double getConfidence() {
            return confidence;
        }

        public void setConfidence(double confidence) {
            this.confidence = confidence;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public int getTagCount() {
            return tagCount;
        }

        public void setTagCount(int tagCount) {
            this.tagCount = tagCount;
        }

        public TagDistEntry clone() {
            return new TagDistEntry(uid, followId, sum, confidence, tag, tagCount);
        }

        public static List<TagDistEntry> parseTagDist(String uid, String followId, TagDistSer t) {
            LinkedList<TagDistEntry> list = new LinkedList<>();
            for (Map.Entry<String, Integer> e : t.getTagCount().entrySet()) {
                TagDistEntry entry = new TagDistEntry(uid, followId, t.getSum(), t.getConfidence(), e);
                entry.tagWeight = entry.confidence * (alpha + entry.tagCount) / t.getSum();
                list.add(entry);
            }
            return list;
        }
    }
}
