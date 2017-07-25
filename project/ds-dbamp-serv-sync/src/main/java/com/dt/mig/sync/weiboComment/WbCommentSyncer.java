package com.dt.mig.sync.weiboComment;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConfiguration;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.MigEsWbcmtCommentWriter;
import com.dt.mig.sync.es.MigEsWbcmtWeiboWriter;
import com.dt.mig.sync.hbase.HBaseUtils;
import com.dt.mig.sync.hbase.RFieldGetter;
import com.dt.mig.sync.hbase.doc.Result2DocMapper;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.dt.mig.sync.utils.SentimentUtil;
import com.dt.mig.sync.utils.SparkUtil;
import com.dt.mig.sync.utils.WeiboUtil;
import com.dt.mig.sync.weiboComment.doc.Cmt2EsDocMapper;
import com.dt.mig.sync.weiboComment.doc.CmtParent2EsDocMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * com.dt.mig.sync.weiboComment.WbCommentSyncer
 *
 * @author lhfcws
 * @since 2017/4/5
 */
@Deprecated
public class WbCommentSyncer implements Serializable {
    protected static Logger LOG = Logger.getLogger(WbCommentSyncer.class);
    public int cores = 5;

    public void run(Scan scan) throws IOException {
        //Configuration conf = new Configuration(MigGenConfig.getInstance());
        MigSyncConfiguration conf = MigSyncConfiguration.getInstance();
//        MigConfiguration conf = MigConfiguration.getInstance();
        String appName = this.getClass().getSimpleName();
        StrParams sparkConf = new StrParams();
        sparkConf.put("spark.executor.memory", "2500m");

        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, appName, cores, sparkConf);
        Accumulator<Integer> totalAcc = jsc.accumulator(0);

        String scanStr = HBaseUtils.convertScanToString(scan);
        conf.set(TableInputFormat.SCAN, scanStr);
        conf.set(TableInputFormat.INPUT_TABLE, MigSyncConsts.HBASE_WEIBO_COMMENT_TBL_NEW);

        try {
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    Result result = v1._2();
                    Params p = new Result2DocMapper(result).map();
                    //System.out.println("序列化数据:" + p);
                    return new Gson().toJson(p);
                    //return FastJsonSerializer.serialize(p);
                }
            }).repartition(50).foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iter) throws Exception {
                    RFieldGetter cntGetter = new RFieldGetter(MigSyncConsts.HBASE_WEIBO_CONTENT_TBL_NEW);
                    RFieldGetter userGetter = new RFieldGetter(MigSyncConsts.HBASE_WEIBO_USER_TBL_NEW);
                    //CommonWriter parentWriter = new CommonWriter(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_CONMENT_PARENT_TYPE);
                    //CommonWriter childWriter = new CommonWriter(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_CONMENT_CHILD_TYPE);

                    MigEsWbcmtWeiboWriter parentWriter = MigEsWbcmtWeiboWriter.getInstance();
                    MigEsWbcmtCommentWriter childWriter = MigEsWbcmtCommentWriter.getInstance();

                    Gson gson = new Gson();
                    while (iter.hasNext()) {
                        String json = iter.next();
                        //Params p = FastJsonSerializer.deserialize(json, Params.class);
                        Params p = gson.fromJson(json, new TypeToken<Params>() {
                        }.getType());
                        String uid = p.getString("uid");
                        String mid = p.getString("mid");
                        String cmtid = p.getString("cmt_id");
                        System.out.printf("[WRITE]:UID-%S,MID-%S。\n", uid, mid);
                        System.out.println("[p]:" + p);
                        Params cnt = cntGetter.get(BanyanTypeUtil.wbcontentPK(mid));
                        Params user = userGetter.get(BanyanTypeUtil.sub3PK(uid));
//                                System.out.println("[WRITE CNT] : " + cnt);
//                                System.out.println("[WRITE USER] : " + user);
                        try {
                            CmtParent2EsDocMapper parent2EsDocMapper = new CmtParent2EsDocMapper(cnt);
                            if (user != null) {
                                parent2EsDocMapper.setUserType(user.getString("user_type"));
                            } else {
                                System.out.println("[USER NULL]:" + uid);
                            }
                            // gen parent
                            Params parent = parent2EsDocMapper.map();

                            // gen child
                            Params child = new Cmt2EsDocMapper(p).setParams(cnt, user).map();

                            // write es
                            try {
                                if (parent != null) {
                                    System.out.println("[WRITE PARENT]:" + mid);
                                    YZDoc yzDoc = new YZDoc(parent);
                                    //分析操作
                                    //分析mig_mention
                                    WeiboUtil.sentimentAnaly(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SentimentSourceType.WEIBO);//根据关键词分析本体的情感;对每一个品牌,活动
                                    //WeiboUtil.extractHighFreqyWord(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD);
                                    //移除weibo话题重新计算情感sentiment
                                    WeiboUtil.removeTopicAnalySentiment(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SentimentSourceType.WEIBO);
                                    //检查情感sentiment是否异常,若异常则设为中性
                                    SentimentUtil.checkAndSetSentiment(yzDoc);
                                    System.out.println("[PARENT]:" + yzDoc.toJson());
                                    parentWriter.addData(yzDoc);
                                    System.out.println("[FINISH WRITE PARENT]:" + mid);
                                }
                            } catch (Exception e) {
                                System.out.printf("ERROR PARENT:" + e.getMessage());
                                LOG.error(e.getMessage(), e);
                            }

                            try {
                                if (child != null) {
                                    System.out.println("[WRITE CHILD]:" + cmtid);
                                    YZDoc yzDoc = new YZDoc(child);
                                    //分析操作
                                    //分析mig_mention
                                    WeiboUtil.sentimentAnaly(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD, SentimentSourceType.WEIBO);//根据关键词分析本体的情感;对每一个品牌,活动
                                    //分析其高频词
                                    WeiboUtil.extractHighFreqyWord(yzDoc, MigSyncConsts.DEFAULT_ANALY_FIELD);
                                    //检查情感sentiment是否异常,若异常则设为中性
                                    SentimentUtil.checkAndSetSentiment(yzDoc);
                                    System.out.println("[CHILD]:" + yzDoc.toJson());
                                    childWriter.addData(yzDoc, mid);
                                    System.out.println("[FINISH WRITE CHILD]:" + cmtid);
                                }
                            } catch (Exception e) {
                                System.out.printf("ERROR CHILD:" + e.getMessage());
                                LOG.error(e.getMessage(), e);
                            }

                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }

                    parentWriter.flush();
//                            childWriter.flush();
                    parentWriter.close();
//                            childWriter.close();
                }
            });

            LOG.info("");
            LOG.info("[ACC] Total: " + totalAcc.value());
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    public void run(String startUpdateDate, String endUpdateDate) throws IOException {
        Scan scan = HBaseUtils.buildScan();

        List<Filter> filters = new ArrayList<>();
        if (startUpdateDate != null) {
            filters.add(new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startUpdateDate.getBytes()));
        }

        if (endUpdateDate != null) {
            filters.add(new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, startUpdateDate.getBytes()));
        }

        if (!filters.isEmpty()) {
            scan.setFilter(new FilterList(filters));
        }

        run(scan);
    }

    public void runAll() throws IOException {
        run(null, null);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        WbCommentSyncer runner = new WbCommentSyncer();

        if (args.length >= 2) {
            runner.run(args[0], args[1]);
        } else if (args.length == 1) {
            runner.run(args[0], null);
        } else runner.runAll();
        System.out.println("[PROGRAM] Program exited.");
    }
}
