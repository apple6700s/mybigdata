package com.datastory.banyan.asyncdata.migrate.doc;

import com.datastory.banyan.asyncdata.util.SiteIdMapping;
import com.datastory.banyan.asyncdata.util.SiteTypes;
import com.datastory.banyan.asyncdata.util.UniqueIdGen;
import com.datastory.banyan.doc.ResultRawDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;


/**
 * com.datastory.banyan.asyncdata.migrate.doc.EcomVideoMigrateDocMapper
 * <p>
 * 目前要迁移的表有：
 * "40","dt.rhino.app.radar_comment","{publish_date}_{site}_{parent_id}_{comment_id}","{keyword}","8","jobName,keyword,module,site,item_id,title,content,is_main_post,url,author,view_count,review_count,like_count,dislike_count,same_html_count,province,city,publish_date,update_date,other_data,sourceCrawlerId","","","[数说雷达]评论表"
 * "53","dt.rhino.app.yili2_zongyi_post","{publish_date}_{keyword}_{filter_word}_{update_date}_{site}_{item_id}","{keyword}","8","item_id,is_main_post,keyword,site,title,content,url,author,view_count,review_count,like_count,dislike_count,category,province,city,other_data,guess_date,publish_date,update_date,sourceCrawlerId,same_html_count,jobName,article_type","","","[伊利雷达]2期液奶综艺文章表"
 * "10","dt.rhino.app.yili_zongyi_post","{publish_date}_{keyword}_{filter_word}_{update_date}_{site}_{item_id}","{keyword}","2","item_id,is_main_post,keyword,site,title,content,url,author,view_count,review_count,like_count,dislike_count,category,province,city,other_data,guess_date,publish_date,update_date,srcId,same_html_count,jobName,album_title","","","[伊利]伊利综艺"
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public class EcomVideoMigrateDocMapper extends ResultRawDocMapper {
    protected String table;

    public EcomVideoMigrateDocMapper(String table, Result result) {
        super(result);
        this.table = table;
    }

    protected static String[] DIRECT = {
            "other_data", "publish_date", "update_date",
            "sourceCrawlerId", "province", "city", "title",
            "content", "url", "author",
    };

    protected static Map<String, String> RENAME_MAP = BanyanTypeUtil.strArr2strMap(new String[]{
            "item_id", "item_id",
            "keyword", "crawl_kw",
            "view_count", "view_cnt",
            "review_count", "review_cnt",
            "like_count", "like_cnt",
            "dislike_count", "dislike_cnt",
            "site", "site_name",
    });

    @Override
    public Params map() {
        this.doc = new Params();

        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, RAW_FAMILY);
        fmap.remove("_0");

        BanyanTypeUtil.putAllNotNull(doc, fmap, DIRECT);
        BanyanTypeUtil.putAllNotNull(doc, fmap, RENAME_MAP);


        if (!doc.containsKey("publish_date")) {
            BanyanTypeUtil.safePut(doc, "publish_date", getString("guess_date"));
        }

        if (table.toLowerCase().contains("comment")) {
            doc.put("is_parent", "0");
        } else if (table.toLowerCase().contains("post")) {
            doc.put("is_parent", "1");
        }

        /**
         * Gen site_id
         */
        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                String siteName = doc.getString("site_name");
                if (siteName != null) {
                    try {
                        if (SiteTypes.getInstance().getType(siteName) != null) {
                            String siteId = SiteIdMapping.getSiteId(siteName);
                            BanyanTypeUtil.safePut(doc, "site_id", siteId);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return null;
            }
        });

        /**
         * Gen new pk
         * unique_id生成规则
         * 视频详情：md5(site_id+"_"+item_id)
         * 视频评论：md5(site_id+"_"+item_id)
         * 电商详情：md5(site_id+"_"+item_id)
         * 电商评论：md5(site_id+"_"+comment_id)
         *
         *
         */
        this.function(new Function<Void, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable Void aVoid) {
                String oldPk = getString("pk");
                String siteId = doc.getString("site_id");
                if (table.equals("dt.rhino.app.radar_comment")) {
                    // 尽可能用回同一套id生成体系，不行才自己生成。
                    String[] arr = oldPk.split("\\|");
                    String cmtId = BanyanTypeUtil.safeGet(arr, -1);
                    String parentId = BanyanTypeUtil.safeGet(arr, -2);
                    // 原表里不知道他们怎么存的，cmtId和parentId居然有均为empty的情况，我也是很无奈
                    if (cmtId.equals("empty")) {
                        // 32 + 13 = 45 位的 id ，一般id MD5是32位，这种迁移的比较特殊。
                        BanyanTypeUtil.safePut(doc, "pk", UniqueIdGen.gen(oldPk));
                    } else {
                        BanyanTypeUtil.safePut(doc, "pk", cmtId);
                    }

                    BanyanTypeUtil.safePut(doc, "cmt_id", doc.getString("pk"));

                    if (!parentId.equals("empty")) {
                        BanyanTypeUtil.safePut(doc, "parent_id", parentId);
                    }

                    BanyanTypeUtil.safePut(doc, "is_parent", "0");
                } else if (table.contains("post")) {
                    BanyanTypeUtil.safePut(doc, "pk", UniqueIdGen.gen(siteId, doc.getString("item_id")));
                    BanyanTypeUtil.safePut(doc, "is_parent", "1");
                }
                return null;
            }
        });


        if (!doc.containsKey("pk")) {
            LOG.error("pk=" + getString("pk") + ", " + doc);
            return null;
        }

        return this.doc;
    }
}
