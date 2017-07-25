package com.datastory.banyan.weibo.abel;

import com.datastory.banyan.utils.BanyanTypeUtil;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;

/**
 * com.datatub.rhino.base.table.Tables
 * schema table center : hbase table / phoenix table / es index / mysql table / kafka topic or group
 *
 * @author lhfcws
 * @since 2016/11/2
 */
public class Tables {
    protected static Configuration conf = ConfUtil.resourceConf("schema-config.xml");

    // phoenix better use dash instead of dot
    public static final String PH_WBUSER_TBL = "DS_BANYAN_WEIBO_USER";
    public static final String PH_WBCNT_TBL = "DS_BANYAN_WEIBO_CONTENT";
    public static final String PH_WBCMT_TBL = "DS_BANYAN_WEIBO_COMMENT";
    public static final String PH_LONGTEXT_POST_TBL = "DS_BANYAN_NEWSFORUM_POST";
    public static final String PH_LONGTEXT_CMT_TBL = "DS_BANYAN_NEWSFORUM_COMMENT";
    public static final String PH_WBADVUSER_TBL = "DT_RHINO_WEIBO_ADVUSER";
    public static final String PH_WBTREND_TBL = "DS_BANYAN_WEIBO_TREND";
    public static final String PH_KAFKA_OFFSET_TBL = "dt_rhino_kafka_offset";
    public static final String PH_WXCNT_TBL = "DS_BANYAN_WECHAT_CONTENT";
    public static final String PH_WXMP_TBL = "DS_BANYAN_WECHAT_MP";

    public static final String ANALYZ_USER_TBL = "WEIBO_USER_TEMP_ANALYZ";

    // es index
    public static final String ES_WB_IDX = "ds-banyan-weibo-index";
    public static final String ES_WECHAT_IDX = "ds-banyan-wechat-index";
    public static final String ES_WBCMT_IDX = "ds-banyan-weibo-comment-index";
    public static final String ES_LTEXT_IDX = "ds-banyan-newsforum-index";

    // kafka topic
    public static final String KFK_PK_WB_TP = "topic.banyan.pk.weibo";
    public static final String KFK_PK_WB_GRP = "consumer.banyan.pk.weibo";
//    public static final String KFK_PK_WBUSER_TP = "topic.banyan.pk.weibo.user";
//    public static final String KFK_PK_WBUSER_GRP = "consumer.banyan.pk.weibo.user";
    public static final String KFK_PK_LT_TP = "topic.banyan.pk.newsforum";
    public static final String KFK_PK_LT_GRP = "consumer.banyan.pk.newsforum";
    public static final String KFK_PK_WX_TP = "topic.banyan.pk.wechat";
    public static final String KFK_PK_WX_GRP = "consumer.banyan.pk.wechat";

    public static final String KFK_WB_TP = "topic.rhino.weibo.all";
    public static final String KFK_WB_GRP = "consumer.group.weibo.all";
    public static final String KFK_WBCMT_TP = "topic.rhino.weibo.comment";
    public static final String KFK_WBCMT_GRP = "consumer.group.weibo.comment";
    public static final String KFK_WB_UP_TP = "topic.rhino.weibo.update";
    public static final String KFK_WB_UP_GRP = "consumer.group.weibo.update";
    public static final String KFK_WBUSER_TP = "topic.rhino.weibo.user";
    public static final String KFK_WBUSER_GRP = "consumer.group.weibo.user";
    public static final String KFK_LT_ALL_TP = "topic.rhino.news_bbs.all";
    public static final String KFK_LT_ALL_GRP = "consumer.group.news.all";
    public static final String KFK_WX_CNT_TP = "topic.rhino.wx.cnt";
    public static final String KFK_WX_CNT_GRP = "consumer.group.wx.cnt";


    public static String table(String key) {
        return conf.get(key);
    }

    public static boolean isEnablePK() {
        return conf.getBoolean("kafka.pk.spout.enable", true);
    }

    private static final HashMap<String, String> hbase2esMap = new HashMap<>();
    static {
        hbase2esMap.putAll(BanyanTypeUtil.strArr2strMap(new String[] {
                PH_WBCNT_TBL, ES_WB_IDX,
                PH_WBUSER_TBL, ES_WB_IDX,
                PH_LONGTEXT_POST_TBL, ES_LTEXT_IDX,
                PH_LONGTEXT_CMT_TBL, ES_LTEXT_IDX,
                PH_WXCNT_TBL, ES_WECHAT_IDX,
                PH_WXMP_TBL, ES_WECHAT_IDX,
        }));
    }
    public static String getEsKeyByHBaseKey(String key) {
        return hbase2esMap.get(key);
    }

    public static String getEsTableByHBaseTable(String tbl) {
        for (String key : hbase2esMap.keySet()) {
            String t = Tables.table(key);
            if (t != null && tbl != null && t.equals(tbl)) {
                String esKey = hbase2esMap.get(key);
                return Tables.table(esKey);
            }
        }
        return null;
    }

    public static String getEsType(String hbTable) {
        if (Tables.table(PH_WBCNT_TBL).equals(hbTable)) {
            return "weibo";
        } else if (Tables.table(PH_WXCNT_TBL).equals(hbTable)) {
            return "wechat";
        } else if (Tables.table(PH_WXMP_TBL).equals(hbTable)) {
            return "mp";
        } else if (Tables.table(PH_LONGTEXT_POST_TBL).equals(hbTable)) {
            return "post";
        } else if (Tables.table(PH_LONGTEXT_CMT_TBL).equals(hbTable)) {
            return "comment";
        } else if (Tables.table(PH_WBUSER_TBL).equals(hbTable)) {
            return "user";
        } else
            return null;
    }

    /**
     * test main
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        String tbl = Tables.table(PH_KAFKA_OFFSET_TBL);
        System.out.println("[TABLE] " + tbl);
        System.out.println("[PROGRAM] Program exited.");
    }
}
