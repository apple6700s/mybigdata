package com.dt.mig.sync.questionAnswer;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.hbase.HBaseUtils;
import com.dt.mig.sync.hbase.TrendHBaseReader;
import com.dt.mig.sync.utils.NestedStructureUtil;
import com.dt.mig.sync.utils.QuestionAnswerUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.dt.mig.sync.base.MigSyncConsts.*;


/**
 * com.dt.mig.sync.questionAnswer.QuestionAnswerSyncTrend
 *
 * @author zhaozhen
 * @since 2017/7/6
 */
public class QuestionAnswerSyncTrend {
    public static final int TREAND_MAX_VERSION = 30;
    private static final Logger LOG = LoggerFactory.getLogger(AQuestionAnswerReader.class);

    /**
     * 计算评论点赞的趋势分布
     *
     * @param docs
     * @param idCache
     */
    public static void getTrendDist(List<YZDoc> docs, ArrayList<String> idCache) {

        TrendHBaseReader trendHBaseReader = new TrendHBaseReader();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        //获取点赞分布,评论分布
        for (int i = 0; i < docs.size(); i++) {
            try {
                String id = idCache.get(i);
                YZDoc doc = docs.get(i);
                System.out.println("开始计算id:" + id + "的趋势分布!");
                //System.out.println("params:" + cntReParams.toJson());
                Params params = trendHBaseReader.readTrend(HBaseUtils.qaTrendPK(id), TREAND_MAX_VERSION);

                if (params != null && doc != null && params.containsKey("data")) {
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


                    int prefixLikeTotal = 0;
                    int prefixCommentTotal = 0;
                    int preFixPostLikeTotal = 0;

                    long maxLike = -1;
                    long maxComment = -1;
                    long maxPostLike = -1;

                    List<Map<String, Object>> listLikes = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> listComments = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> listPostLikes = new ArrayList<Map<String, Object>>();
                    LOG.info("开始取值若为" + doc.containsKey("_parent"));
                    if (doc.containsKey("_parent")) {
                        //生成对应的点赞分布,评论分布、转发分布
                        for (Map.Entry<String, Pair<Long, String>> entry : dateToTimestampToTrend.entrySet()) {
                            String updateDate = entry.getKey();
                            String trendValue = entry.getValue().getSecond();
                            Integer[] trendValue1;
                            trendValue1 = QuestionAnswerUtil.getTrendValue(trendValue);
                            Integer likeValue;
                            if (trendValue1[0] != null) {
                                likeValue = trendValue1[0];
                                LOG.info("点赞趋势值为：" + likeValue);
                                if (likeValue != null && maxLike < likeValue) {
                                    maxLike = likeValue;
                                }

                                NestedStructureUtil.parseTrend(listLikes, updateDate, likeValue - prefixLikeTotal);
                                prefixLikeTotal = likeValue;
                            }
                            Integer commentValue;
                            if (trendValue1[1] != null) {
                                commentValue = trendValue1[1];

                                if (commentValue != null && maxComment < commentValue) {
                                    maxComment = commentValue;
                                }
                                NestedStructureUtil.parseTrend(listComments, updateDate, commentValue - prefixCommentTotal);
                                prefixCommentTotal = commentValue;
                            }

                        }
                        //插入到对应的params
                        LOG.info("----------插入点赞评论趋势值" + listLikes + "----------------"+ listComments);

                        doc.put(ES_NEWS_ANSWER_COMMENT_LIKE, listLikes);

                        doc.put(ES_NEWS_ANSWER_COMMENT_COUNT, listComments);

//                        if (maxComment >= 0) {
//                            doc.put(ES_NEWS_ANSWER_COMMENT_CNT, maxComment);
//                        }

                        LOG.info("[趋势字段doc：]" + doc.toJson());

                        LOG.info("[[TREND]like:%s,comment:%s\n]", listLikes, listComments);
                        System.out.printf("[TREND]like:%s,comment:%s\n", listLikes, listComments);
                    } else {
                        //生成对应的点赞分布,评论分布、转发分布
                        for (Map.Entry<String, Pair<Long, String>> entry : dateToTimestampToTrend.entrySet()) {
                            String updateDate = entry.getKey();
                            String trendValue = entry.getValue().getSecond();
                            Integer[] trendValue1;
                            trendValue1 = QuestionAnswerUtil.getTrendValue(trendValue);
                            Integer postLikeValue;
                            if (trendValue1[1] != null) {
                                postLikeValue = trendValue1[1];
                                if (postLikeValue != null && maxPostLike < postLikeValue) {
                                    maxPostLike = postLikeValue;
                                }
                                NestedStructureUtil.parseTrend(listPostLikes, updateDate, postLikeValue - preFixPostLikeTotal);
                                preFixPostLikeTotal = postLikeValue;
                            }
                        }
                        //插入到对应的params
                        doc.put(ES_NEWS_QUESTION_POST_LIKE, listPostLikes);

                        System.out.printf("[TREND]like:%s\n", listPostLikes);
                    }

                } else {
                    if (doc == null) {
                        System.out.println("cntReParams!id:" + id);
                    }
//                    System.out.println(params == null ? "param等于空!mid:" + mid : params.toJson());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

}
