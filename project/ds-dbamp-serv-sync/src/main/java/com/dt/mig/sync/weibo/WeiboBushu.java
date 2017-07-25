package com.dt.mig.sync.weibo;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.es.CommonReader;
import com.dt.mig.sync.es.CommonWriter;
import com.dt.mig.sync.hbase.HBaseReader;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;

/**
 * Created by abel.chan on 17/2/23.
 */
@Deprecated
public class WeiboBushu {

    private final static String index = "dt-rhino-weibo-mig-v5";
    private final static String type = "weibo";

    public QueryBuilder buildQueryBuilder() throws Exception {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.existsQuery("retweet_id"));
        queryBuilder.mustNot(QueryBuilders.existsQuery("retweet_content"));
        return queryBuilder;
    }

    public List<WeiboObj> getMissingRetContentData() throws Exception {
        try {
            QueryBuilder queryBuilder = buildQueryBuilder();
            CommonReader reader = CommonReader.getInstance();
            long count = reader.getTotalHits(index, type, queryBuilder);
            System.out.println("满足条件的总共有:" + count);
            List<WeiboObj> datas = new ArrayList<WeiboObj>();

            EsReaderResult esReaderResult = reader.scroll(index, type, queryBuilder, new String[]{"id", "retweet_id"}, new String[]{});
            while (!esReaderResult.isEnd()) {
                if (esReaderResult != null && esReaderResult.getDatas() != null) {
                    datas.addAll(objWrapper(esReaderResult.getDatas()));
                }
                esReaderResult = reader.search(esReaderResult.getScrollId());
            }
            return datas;
        } catch (Exception e) {
            throw e;
        }
    }

    public void getRetweetContents(List<WeiboObj> weiboObjs) throws Exception {

        HBaseReader cntReader = new HBaseReader() {
            @Override
            public String getTable() {
                return "DS_BANYAN_WEIBO_CONTENT_V1";
            }
        };
        Map<String, String> rtMidToRtContent = new HashMap<>();
        int unExistsContentCount = 0;
        for (WeiboObj obj : weiboObjs) {
            if (StringUtils.isNotEmpty(obj.rtMid)) {
                List<Params> cntRes = cntReader.batchRead(BanyanTypeUtil.wbcontentPK(obj.rtMid));
                if (!CollectionUtil.isEmpty(cntRes)) {
                    for (Params cntRe : cntRes) {
                        if (cntRe == null) {
                            unExistsContentCount++;
                            continue;
                        }
                        String content = cntRe.getString("content");
                        String mid = cntRe.getString("mid");
                        if (StringUtils.isNotEmpty(mid) && StringUtils.isNotEmpty(content)) {
                            rtMidToRtContent.put(mid, content);
                        }
                    }
                }
            }
        }

        List<Params> cntRes = cntReader.flush();
        if (!CollectionUtil.isEmpty(cntRes)) {
            for (Params cntRe : cntRes) {
                if (cntRe == null) {
                    unExistsContentCount++;
                    continue;
                }
                String content = cntRe.getString("content");
                String mid = cntRe.getString("mid");
                if (StringUtils.isNotEmpty(mid) && StringUtils.isNotEmpty(content)) {
                    rtMidToRtContent.put(mid, content);
                }
            }
        }

        for (WeiboObj obj : weiboObjs) {
            if (StringUtils.isNotEmpty(obj.rtMid) && rtMidToRtContent.containsKey(obj.rtMid)) {

                obj.rtContent = rtMidToRtContent.get(obj.rtMid);
            }
        }
        System.out.println("存在rt_mid,但是不存在rt_content的个数:" + unExistsContentCount);
    }

    public List<WeiboObj> objWrapper(SearchHit[] hits) {
        List<WeiboObj> datas = new ArrayList<WeiboObj>();
        if (hits != null) {
            for (SearchHit hit : hits) {
                String parentId = hit.getFields().get(MigSyncConsts.PARENT_ID).getValue();
                if (StringUtils.isEmpty(parentId)) continue;
                String id = hit.getId();
                if (StringUtils.isEmpty(id)) continue;
                Map<String, Object> metadata = hit.getSource();
                if (metadata != null && metadata.containsKey("retweet_id")) {
                    WeiboObj obj = new WeiboObj();
                    obj.mid = id;
                    obj.rtMid = metadata.get("retweet_id").toString();
                    obj.uid = parentId;
                    datas.add(obj);
                }
            }
        }
        return datas;
    }

    public void updateEs(List<WeiboObj> weiboObjs) {
        final CommonWriter cntEsWriter = new CommonWriter(index, type);
        if (weiboObjs == null) {
            return;
        }
        try {
            for (WeiboObj obj : weiboObjs) {
                if (StringUtils.isNotEmpty(obj.mid) && StringUtils.isNotEmpty(obj.uid) && StringUtils.isNotEmpty(obj.rtContent)) {
                    YZDoc yzDoc = new YZDoc();
                    yzDoc.put("id", obj.mid);
                    yzDoc.put("retweet_content", obj.rtContent);
                    cntEsWriter.updateData(yzDoc, obj.uid);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cntEsWriter.flush();
            cntEsWriter.close();
        }
    }


    public static void main(String[] args) {
        try {
            WeiboBushu bushu = new WeiboBushu();
            List<WeiboObj> datas = bushu.getMissingRetContentData();
            System.out.println("读数完毕!");
            bushu.getRetweetContents(datas);
            //System.out.println(datas.toString());
            int exist = 0;
            Set<String> unExistContents = new HashSet<String>();
            for (WeiboObj weiboObj : datas) {
                if (StringUtils.isNotEmpty(weiboObj.rtMid) && StringUtils.isNotEmpty(weiboObj.rtContent)) {
                    exist++;
                } else if (StringUtils.isNotEmpty(weiboObj.rtMid)) {
                    unExistContents.add(weiboObj.rtMid);
                }
            }
            System.out.println("总共不需要写rtContent的是:" + unExistContents);
            System.out.println(unExistContents);
            System.out.println("需写入库的条数:" + exist);
            //bushu.updateEs(datas);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected class WeiboObj {
        String uid;
        String mid;
        String rtMid;
        String rtContent;

        @Override
        public String toString() {
            return "WeiboObj{" + "uid='" + uid + '\'' + ", mid='" + mid + '\'' + ", rtMid='" + rtMid + '\'' + ", rtContent='" + rtContent + '\'' + '}';
        }
    }
}
