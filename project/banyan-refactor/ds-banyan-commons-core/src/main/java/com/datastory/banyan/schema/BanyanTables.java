package com.datastory.banyan.schema;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.schema.BanyanTables
 *
 * @author lhfcws
 * @since 2017/5/16
 */
public class BanyanTables {
    public static String ES_HOSTS_ONLY = "alps61,alps62,alps63,todi1,todi2,todi3,todi4,todi5,todi6,todi7,todi8,todi9,todi10,todi11,todi12,todi13,todi14,todi16,todi17,todi18,todi19,todi20,todi21,todi22,todi23,todi24,todi25,todi26,todi27,todi28,todi29,todi30,todi31,todi32,todi33,todi34,todi35,todi36,todi37,todi38,todi39,todi40,todi41,todi42,todi43,todi44,todi45,todi46,todi47,todi48";
    public static String[] WEIBO_FIELDS = {
            //weibo
            "mid", "uid", "url", "self_content", "content", "src_content", "src_uid", "src_mid", "rt_mid", "msg_type",
            "reposts_cnt", "comments_cnt", "attitudes_cnt", "publish_date", "update_date", "source", "sentiment", "keywords",
            "is_ad", "mention", "topics", "emoji",

            // user
            "name", "verified_type", "vtype", "province", "city", "city_level", "gender", "birthyear", "birthdate", "url",
            "constellation", "fans_level", "fans_cnt", "follow_cnt", "wb_cnt", "bi_follow_cnt", "fav_cnt", "user_type",
            "company", "school", "activeness", "meta_group"
    };
    public static String[] WEIXIN_FIELDS = {
            "biz", "content", "author", "title", "url", "like_cnt", "view_cnt", "is_original", "thumbnail", "brief",
            "sentiment", "is_ad", "keywords", "article_id", "sn", "idx", "mid", "wxid", "wx_author", "publish_date", "update_date"
    };
    public static String[] NEWS_FIELDS = {
            "cat_id", "title", "content", "view_cnt", "author", "review_cnt", "site_id", "site_name", "publish_date", "update_date",
            "source",  "is_recom", "is_top", "is_hot", "is_digest", "sentiment", "keywords", "is_ad", "is_robot"
    };

    private static Params sourceTableMap = BanyanTypeUtil.objArr2strMap(new Object[]{
            "weibo", Tables.table(Tables.PH_WBCNT_TBL),
            "weixin", Tables.table(Tables.PH_WXCNT_TBL),
            "wechat", Tables.table(Tables.PH_WXCNT_TBL),
//            "newsforum", new String[]{Tables.table(Tables.PH_LONGTEXT_POST_TBL), Tables.table(Tables.PH_LONGTEXT_CMT_TBL)},
            "newsforum", Tables.table(Tables.PH_LONGTEXT_POST_TBL),
            "newsforum-post", Tables.table(Tables.PH_LONGTEXT_POST_TBL),
            "newsforum-comment", Tables.table(Tables.PH_LONGTEXT_CMT_TBL),
            "ecom", Tables.table(Tables.PH_ECOM_CMT_TBL),
            "video", Tables.table(Tables.PH_VIDEO_CMT_TBL)
    });

    public static String[] source2table(String source) {
        if (source == null)
            return null;
        Object o = sourceTableMap.get(source);
        if (o instanceof String) {
            return new String[]{(String) o};
        } else
            return (String[]) o;
    }

    private static Params sourceIndexMap = BanyanTypeUtil.objArr2strMap(new Object[]{
            "weibo", Tables.table(Tables.ES_WB_IDX) + ".weibo",
            "weixin", Tables.table(Tables.ES_WECHAT_IDX) + ".wechat",
            "wechat", Tables.table(Tables.ES_WECHAT_IDX) + ".wechat",
//            "newsforum", new String[]{Tables.table(Tables.ES_LTEXT_IDX) + ".post", Tables.table(Tables.ES_LTEXT_IDX) + ".comment"},
            "newsforum", Tables.table(Tables.ES_LTEXT_IDX) + ".post",
            "newsforum-post", Tables.table(Tables.ES_LTEXT_IDX) + ".post",
            "newsforum-comment", Tables.table(Tables.ES_LTEXT_IDX) + ".comment",
            "ecom", Tables.table(Tables.ES_ECOM_IDX) + ".comment",
            "video", Tables.table(Tables.ES_VIDEO_IDX) + ".comment",
    });

    public static String[] source2index(String source) {
        if (source == null)
            return null;
        Object o = sourceIndexMap.get(source);
        if (o instanceof String) {
            return new String[]{(String) o};
        } else
            return (String[]) o;
    }

    public static String getIndex(String indexOrIdxType) {
        String[] arr = indexOrIdxType.split("\\.");
        return arr[0];
    }

    public static String findHBaseTable(String esIndex, String esType) {
        if (esIndex.contains("weibo-index") && esType.equals("weibo"))
            return Tables.table(Tables.PH_WBCNT_TBL);
        else if (esIndex.contains("wechat-index") && esType.equals("wechat"))
            return Tables.table(Tables.PH_WXCNT_TBL);
        else if (esIndex.contains("newsforum-index") && esType.equals("post"))
            return Tables.table(Tables.PH_LONGTEXT_POST_TBL);
        else if (esIndex.contains("newsforum-index") && esType.equals("comment"))
            return Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
        else if (esIndex.contains("video-index") && esType.equals("comment"))
            return Tables.table(Tables.PH_VIDEO_CMT_TBL);
        else if (esIndex.contains("video-index") && esType.equals("post"))
            return Tables.table(Tables.PH_VIDEO_POST_TBL);
        else if (esIndex.contains("ecom-index") && esType.equals("comment"))
            return Tables.table(Tables.PH_ECOM_CMT_TBL);
        else return null;
    }

    public static String findUserHBaseTable(String cntTable) {
        if (Tables.table(Tables.PH_WBCNT_TBL).equals(cntTable))
            return Tables.table(Tables.PH_WBUSER_TBL);
        else return null;
    }
}
