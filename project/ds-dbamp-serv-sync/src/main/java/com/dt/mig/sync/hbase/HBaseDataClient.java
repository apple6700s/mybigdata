package com.dt.mig.sync.hbase;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.ds.dbamp.core.utils.ESUtils;
import com.dt.mig.sync.base.MigSyncConfiguration;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.extract.MsgTypeExtractor;
import com.dt.mig.sync.extract.SelfContentExtractor;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.*;

/**
 * Created by abel.chan on 16/11/20.
 */
public class HBaseDataClient {

    private static final Logger log = Logger.getLogger(HBaseDataClient.class);

    private static HBaseDataClient hBaseDataClient;

    private HConnection hconn = null;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    private static final DateTimeFormatter BIRTH_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    private HBaseDataClient() {
        try {
            hconn = HConnectionManager.createConnection(MigSyncConfiguration.getInstance());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static HBaseDataClient getInstance() {
        if (hBaseDataClient == null) {
            synchronized (HBaseDataClient.class) {
                if (hBaseDataClient == null) {
                    hBaseDataClient = new HBaseDataClient();
                }
            }
        }
        return hBaseDataClient;
    }

    /**
     * 微博内容表
     *
     * @return
     * @throws Exception
     */
    private HTableInterface getWeiboContentTable() throws Exception {
        if (hconn != null) {
            return hconn.getTable(MigSyncConsts.HBASE_WEIBO_CONTENT_TBL_NEW);
        }
        throw new NullPointerException("hConnetion is null!");
    }

    /**
     * 微博用户表
     *
     * @return
     * @throws Exception
     */
    private HTableInterface getWeiboUserTable() throws Exception {
        if (hconn != null) {
            return hconn.getTable(MigSyncConsts.HBASE_WEIBO_USER_TBL_NEW);
        }
        throw new NullPointerException("hConnetion is null!");
    }

    /**
     * 微博评论表
     *
     * @return
     * @throws Exception
     */
    private HTableInterface getWeiboCommentTable() throws Exception {
        if (hconn != null) {
            return hconn.getTable(MigSyncConsts.HBASE_WEIBO_COMMENT_TBL_NEW);
        }
        throw new NullPointerException("hConnetion is null!");
    }

    /**
     * 新闻论坛表
     *
     * @return
     * @throws Exception
     */
    private HTableInterface getNewsForumTable() throws Exception {
        if (hconn != null) {
            return hconn.getTable(MigSyncConsts.HBASE_NEWS_FORUM_POST_TBL_NEW);
        }
        throw new NullPointerException("hConnetion is null!");
    }

    /**
     * 新闻论坛评论表
     *
     * @return
     * @throws Exception
     */
    private HTableInterface getNewsForumCommentTable() throws Exception {
        if (hconn != null) {
            return hconn.getTable(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_TBL_NEW);
        }
        throw new NullPointerException("hConnetion is null!");
    }


    /**
     * 回收表资源
     *
     * @param hTableInterface
     */
    private void closeHtable(HTableInterface hTableInterface) {
        try {
            if (hTableInterface != null) {
                hTableInterface.close();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 获取hbase的主键
     *
     * @param rks
     * @return
     */
    private List<Get> listGet(Collection<String> rks) {
        List<Get> gets = new ArrayList<Get>(rks.size());
        for (String rk : rks) {
            gets.add(new Get(Bytes.toBytes(rk)));
        }
        return gets;
    }

    private List<Get> listGet(Collection<String> rks, byte[] family, byte[][] columns) {
        List<Get> gets = new ArrayList<Get>(rks.size());
        for (String rk : rks) {
            Get get = new Get(Bytes.toBytes(rk));
            if (columns != null) {
                int size = columns.length;
                for (int i = 0; i < size; i++) {
                    get.addColumn(family, columns[i]);
                }
            }
            gets.add(get);
        }
        return gets;
    }


    public void batchGetUserNames(List<YZDoc> weibos) throws Exception {
        Map<String, String> uid2Rk = new HashMap<>();
        for (YZDoc weibo : weibos) {
            if (weibo.containsKey(MigSyncConsts.PARENT_ID) && weibo.get(MigSyncConsts.PARENT_ID) != null) {
                String uid = weibo.get(MigSyncConsts.PARENT_ID).toString();
                uid2Rk.put(uid, ESUtils.genRowKeyForUid(uid));
            }
        }

        List<Get> gets = listGet(uid2Rk.values(), MigSyncConsts.B_HBASE_WEIBO_USER_FAMILY, new byte[][]{MigSyncConsts.B_HBASE_WEIBO_USER_QUALIFY_NAME});

        Map<String, Map<String, String>> contentMap = getWeiboUserData(gets, MigSyncConsts.HBASE_WEIBO_USER_FAMILY, Arrays.asList(MigSyncConsts.HBASE_WEIBO_USER_QUALIFY_NAME));

        for (YZDoc weibo : weibos) {
            if (weibo.containsKey(MigSyncConsts.PARENT_ID) && weibo.get(MigSyncConsts.PARENT_ID) != null) {
                String uid = weibo.get(MigSyncConsts.PARENT_ID).toString();
                if (contentMap.containsKey(uid2Rk.get(uid))) {
                    weibo.put(MigSyncConsts.ES_FIELD_WEIBO_WEIBO_USER_NAME, contentMap.get(uid2Rk.get(uid)).get(MigSyncConsts.HBASE_WEIBO_USER_QUALIFY_NAME));
                }
            }

        }
    }


    public void batchGetWeibos(List<YZDoc> weibos) throws Exception {
        Map<String, String> mid2Rk = new HashMap<>();

        for (YZDoc weibo : weibos) {
            mid2Rk.put(weibo.getId(), ESUtils.genRowKeyForMid(weibo.getId()));
            if (weibo.containsKey(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID)) {
                String rtId = weibo.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID).toString();
                mid2Rk.put(rtId, ESUtils.genRowKeyForMid(rtId));
            }
        }
//        System.out.println(mid2Rk);
        //更改新的hbase
        Map<String, Map<String, String>> contentMap = getWeiboContentData(mid2Rk.values(), MigSyncConsts.HBASE_WEIBO_WEIBO_FAMILY, Collections.singletonList(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));

        for (YZDoc weibo : weibos) {
            String mid = weibo.getId();
            if (contentMap.containsKey(mid2Rk.get(mid))) {
                weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_CONTENT, contentMap.get(mid2Rk.get(mid)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));
            }

            if (weibo.containsKey(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID)) {
                String rtId = weibo.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID).toString();
                if (contentMap.containsKey(mid2Rk.get(rtId))) {
                    weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT, contentMap.get(mid2Rk.get(rtId)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));
                }
            }
        }
    }

    /**
     * 获取weicomment的父文档weibo数据
     *
     * @param weibos
     * @throws Exception
     */
    public void batchGetWCWeibos(List<YZDoc> weibos) throws Exception {
        Map<String, String> mid2Rk = new HashMap<>();

        for (YZDoc weibo : weibos) {
            mid2Rk.put(weibo.getId(), ESUtils.genRowKeyForMid(weibo.getId()));
            if (weibo.containsKey(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID)) {
                String rtId = weibo.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID).toString();
                mid2Rk.put(rtId, ESUtils.genRowKeyForMid(rtId));
            }
        }
//        System.out.println(mid2Rk);
        //更改新的hbase schema
        Map<String, Map<String, String>> contentMap = getWeiboContentData(mid2Rk.values(), MigSyncConsts.HBASE_WEIBO_WEIBO_FAMILY, Arrays.asList(MigSyncConsts.HBASE_WEIBO_WEIBO_QUALIFYS));

        for (YZDoc weibo : weibos) {
            String mid = weibo.getId();
            if (contentMap.containsKey(mid2Rk.get(mid))) {
                weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_CONTENT, contentMap.get(mid2Rk.get(mid)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));
                weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_UID, contentMap.get(mid2Rk.get(mid)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_UID));
            }

            if (weibo.containsKey(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID)) {
                String rtId = weibo.get(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID).toString();
                if (contentMap.containsKey(mid2Rk.get(rtId))) {
                    weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT, contentMap.get(mid2Rk.get(rtId)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));
                    weibo.put(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_UID, contentMap.get(mid2Rk.get(mid)).get(MigSyncConsts.HBASE_WEIBO_WEIBO_UID));
                }
            }
        }
    }


    /**
     * 获取weicomment的子文档comment数据
     *
     * @param weiboComments
     * @throws Exception
     */
    public void batchGetWCComments(List<YZDoc> weiboComments) throws Exception {
        Set<String> rowkeys = new HashSet<>();
        for (YZDoc comment : weiboComments) {
            rowkeys.add(ESUtils.genRowKeyForCMTMid(comment.getId()));
        }

//        System.out.println(mid2Rk);
        Map<String, Map<String, String>> contentMap = getWeiboCommentData(rowkeys, MigSyncConsts.HBASE_WEIBO_COMMENT_COMMENT_FAMLIY, Collections.singletonList(MigSyncConsts.HBASE_WEIBO_COMMENT_COMMENT_CONTENT));

        if (contentMap != null) {
            for (YZDoc comment : weiboComments) {
                String pk = ESUtils.genRowKeyForCMTMid(comment.getId());
                if (contentMap.containsKey(pk)) {
                    comment.put(MigSyncConsts.ES_WEIBO_COMMENT_COMMENT_CONTENT, contentMap.get(pk).get(MigSyncConsts.HBASE_WEIBO_COMMENT_COMMENT_CONTENT));
                }
            }
        }
    }


    /**
     * 获取weicomment的子文档comment对应的微博信息
     *
     * @param weiboComments
     * @throws Exception
     */
    public void batchGetExtraWCComments(List<YZDoc> weiboComments) throws Exception {

        Map<String, String> mid2Rk = new HashMap<>();
        for (YZDoc weiboComment : weiboComments) {
            if (weiboComment.containsKey(MigSyncConsts.PARENT_ID) && weiboComment.get(MigSyncConsts.PARENT_ID) != null) {
                String mid = weiboComment.get(MigSyncConsts.PARENT_ID).toString();
                mid2Rk.put(mid, ESUtils.genRowKeyForMid(mid));
            }
        }

        List<Get> gets = listGet(mid2Rk.values(), MigSyncConsts.B_HBASE_WEIBO_WEIBO_FAMILY, new byte[][]{MigSyncConsts.B_HBASE_WEIBO_WEIBO_REPOST_MID, MigSyncConsts.B_HBASE_WEIBO_WEIBO_SRC_MID, MigSyncConsts.B_HBASE_WEIBO_WEIBO_CONTENT});
        //更新新的hbase schema
        Map<String, Map<String, String>> contentMap = getWeiboContentData(gets, MigSyncConsts.HBASE_WEIBO_WEIBO_FAMILY, Arrays.asList(MigSyncConsts.HBASE_WEIBO_WEIBO_REPOST_MID, MigSyncConsts.HBASE_WEIBO_WEIBO_SRC_MID, MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT));

        for (YZDoc weiboComment : weiboComments) {
            if (weiboComment.containsKey(MigSyncConsts.PARENT_ID) && weiboComment.get(MigSyncConsts.PARENT_ID) != null) {
                String mid = weiboComment.get(MigSyncConsts.PARENT_ID).toString();
                if (contentMap.containsKey(mid2Rk.get(mid))) {
                    Map<String, String> qualifys = contentMap.get(mid2Rk.get(mid));
                    String content = qualifys.get(MigSyncConsts.HBASE_WEIBO_WEIBO_CONTENT);
                    String rtMid = qualifys.get(MigSyncConsts.HBASE_WEIBO_WEIBO_REPOST_MID);
                    String srcMid = qualifys.get(MigSyncConsts.HBASE_WEIBO_WEIBO_SRC_MID);
                    String selftContent = SelfContentExtractor.extract(content);
                    short msgType = MsgTypeExtractor.analyz(srcMid, rtMid, selftContent, content);

                    if (StringUtils.isNotEmpty(mid)) {
                        weiboComment.put(MigSyncConsts.ES_WEIBO_WEIBO_MID, mid);
                    }

                    if (StringUtils.isNotEmpty(rtMid)) {
                        weiboComment.put(MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_ID, rtMid);
                    }
                    weiboComment.put(MigSyncConsts.ES_WEIBO_WEIBO_COMMENT_MSG_TYPE, msgType);
                }
            }

        }
    }


    public void batchGetNewsForumPosts(List<YZDoc> posts) throws Exception {
        Set<String> pks = new HashSet<>();
        for (YZDoc post : posts) {
            pks.add(ESUtils.genRowKeyForPostid(post.getId()));
        }

        Map<String, Map<String, String>> contentMap = getNewsForumPostData(pks, MigSyncConsts.HBASE_NEWS_FORUM_POST_FAMLIY, Arrays.asList(MigSyncConsts.HBASE_NEWS_FORUM_POST_CONTENT, MigSyncConsts.HBASE_NEWS_FORUM_POST_TITLE, MigSyncConsts.HBASE_NEWS_FORUM_POST_VIEW_COUNT, MigSyncConsts.HBASE_NEWS_FORUM_POST_REVIEW_COUNT));

        for (YZDoc post : posts) {
            String pk = post.getId();
            if (contentMap.containsKey(pk)) {
                String title = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_TITLE);
                if (StringUtils.isNotEmpty(title)) {
                    post.put(MigSyncConsts.ES_NEWS_FORUM_POST_TITLE, title);
                    post.put(MigSyncConsts.ES_NEWS_FORUM_POST_TITLE_SOURCE, title);
                    post.put(MigSyncConsts.ES_NEWS_FORUM_POST_TITLE_LENGTH, title.length());
                }
                String mainPost = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_CONTENT);
                post.put(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST, mainPost);
                if (mainPost != null) {
                    post.put(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST_LENGTH, mainPost.length());
                }

                //拿到review_count 和 view_count
                post.put(MigSyncConsts.ES_NEWS_FORUM_REVIEW_COUNT, BanyanTypeUtil.parseIntForce(contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_REVIEW_COUNT)));

                post.put(MigSyncConsts.ES_NEWS_FORUM_VIEW_COUNT, BanyanTypeUtil.parseIntForce(contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_VIEW_COUNT)));
            }
        }
    }

    public void batchGetNewsForumComments(List<YZDoc> comments) throws Exception {
        Set<String> pks = new HashSet<>();
        for (YZDoc comment : comments) {
            pks.add(ESUtils.genRowKeyForCommentid(comment.getId()));
        }

        Map<String, Map<String, String>> contentMap = getNewsForumCommentData(pks, MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_FAMLIY, Arrays.asList(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT));

        for (YZDoc comment : comments) {
            String pk = comment.getId();
            if (contentMap.containsKey(pk)) {
                if (contentMap.get(pk).containsKey(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT)) {
                    String content = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT);
                    if (StringUtils.isNotEmpty(content)) {
                        comment.put(MigSyncConsts.ES_NEWS_FORUM_COMMENT_CONTENT, content);
                        comment.put(MigSyncConsts.ES_NEWS_FORUM_COMMENT_CONTENT_LENGTH, content.length());
                    }
                }
            }
        }
    }

    public String batchGetNewsForumAllComment(List<String> ids) throws Exception {
        List<String> rowkeys = new ArrayList<String>();
        for (String id : ids) {
            rowkeys.add(ESUtils.genRowKeyForCommentid(id));
        }
        Map<String, Map<String, String>> contentMap = getNewsForumCommentData(rowkeys, MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_FAMLIY, Arrays.asList(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT));

        if (contentMap != null && contentMap.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (Map<String, String> entry : contentMap.values()) {
                if (entry.containsKey(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT)) {
                    sb.append(entry.get(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT));
                }
            }
            return sb.toString();
        }
        return "";
    }

    //问答类，主贴内容
    public void batchGetNewsForumQuestionPosts(List<YZDoc> posts) throws Exception {
        Set<String> pks = new HashSet<>();
        for (YZDoc post : posts) {
            pks.add(ESUtils.genRowKeyForPostid(post.getId()));
        }

        Map<String, Map<String, String>> contentMap = getNewsForumPostData(pks, MigSyncConsts.HBASE_NEWS_FORUM_POST_FAMLIY, Arrays.asList(MigSyncConsts.HBASE_NEWS_FORUM_POST_CONTENT, MigSyncConsts.HBASE_NEWS_FORUM_POST_TITLE));

        for (YZDoc post : posts) {
            String pk = post.getId();
            if (contentMap.containsKey(pk)) {
                String title = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_TITLE);
                if (StringUtils.isNotEmpty(title)) {
                    post.put(MigSyncConsts.ES_NEWS_FORUM_POST_TITLE, title);
                }
                String content = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_POST_CONTENT);
                post.put(MigSyncConsts.ES_NEWS_QUESTION_POST_CONTENT, content);
            }
        }
    }

    //问答类，评论内容
    public void batchGetNewsForumAnswerComments(List<YZDoc> comments) throws Exception {
        Set<String> pks = new HashSet<>();
        for (YZDoc comment : comments) {
            pks.add(ESUtils.genRowKeyForCommentid(comment.getId()));
        }

        Map<String, Map<String, String>> contentMap = getNewsForumCommentData(pks, MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_FAMLIY, Arrays.asList(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT, MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_TITLE));

        for (YZDoc comment : comments) {
            String pk = comment.getId();
            if (contentMap.containsKey(pk)) {
                String title = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_TITLE);
                if (StringUtils.isNotEmpty(title)){
                    comment.put(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_TITLE, title);
                }
                String content = contentMap.get(pk).get(MigSyncConsts.HBASE_NEWS_FORUM_COMMENT_CONTENT);
                comment.put(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CONTENT, content);
            }
        }
    }


    private Map<String, Map<String, String>> getNewsForumPostData(Collection<String> rowkeys, String family, List<String> qualifiers) throws Exception {
        HTableInterface newForumTable = null;
        try {
            List<Get> gets = listGet(rowkeys);
            newForumTable = getNewsForumTable();
            Result[] results = newForumTable.get(gets);
            return formatResult(results, family, qualifiers);
        } finally {
            if (newForumTable != null) {
                closeHtable(newForumTable);
            }

        }
    }

    private Map<String, Map<String, String>> getNewsForumCommentData(Collection<String> rowkeys, String family, List<String> qualifiers) throws Exception {
        HTableInterface newForumTable = null;
        try {
            List<Get> gets = listGet(rowkeys);
            newForumTable = getNewsForumCommentTable();
            Result[] results = newForumTable.get(gets);
            return formatResult(results, family, qualifiers);
        } finally {
            if (newForumTable != null) {
                closeHtable(newForumTable);
            }

        }
    }


    private Map<String, Map<String, String>> getWeiboCommentData(Collection<String> rowkeys, String family, List<String> qualifiers) throws Exception {
        HTableInterface weiboCommentTable = null;
        try {
            List<Get> gets = listGet(rowkeys);
            weiboCommentTable = getWeiboCommentTable();
            Result[] results = weiboCommentTable.get(gets);
            return formatResult(results, family, qualifiers);
        } finally {
            if (weiboCommentTable != null) {
                closeHtable(weiboCommentTable);
            }
        }
    }


    private Map<String, Map<String, String>> getWeiboContentData(Collection<String> rowkeys, String family, List<String> qualifiers) throws Exception {
        List<Get> gets = listGet(rowkeys);
        return getWeiboContentData(gets, family, qualifiers);
    }

    private Map<String, Map<String, String>> getWeiboContentData(List<Get> gets, String family, List<String> qualifiers) throws Exception {
        HTableInterface weiboContentTable = null;
        try {
            weiboContentTable = getWeiboContentTable();
            Result[] results = weiboContentTable.get(gets);
            return formatResult(results, family, qualifiers);
        } finally {
            if (weiboContentTable != null) {
                closeHtable(weiboContentTable);
            }
        }
    }

    private Map<String, Map<String, String>> getWeiboUserData(List<Get> gets, String family, List<String> qualifiers) throws Exception {
        HTableInterface weiboUserTable = null;
        try {
            weiboUserTable = getWeiboUserTable();
            Result[] results = weiboUserTable.get(gets);
            return formatResult(results, family, qualifiers);
        } finally {
            if (weiboUserTable != null) {
                closeHtable(weiboUserTable);
            }
        }
    }


    private Map<String, Map<String, String>> formatResult(Result[] results, String family, List<String> qualifiers) {
        if (results != null) {
            Map<String, Map<String, String>> list = new HashMap<>();

            byte[] bFamily = Bytes.toBytes(family);
            Map<String, byte[]> qMap = new HashMap<>();
            for (String qualifier : qualifiers) {
                qMap.put(qualifier, Bytes.toBytes(qualifier));
            }

            for (Result result : results) {
                Map<String, String> map = new HashMap<>();
                for (Map.Entry<String, byte[]> entry : qMap.entrySet()) {
                    map.put(entry.getKey(), HBaseUtils.getValue(result, bFamily, entry.getValue()));
                }
                list.put(Bytes.toString(result.getRow()), map);
            }

            return list;
        }

        return null;
    }

}
