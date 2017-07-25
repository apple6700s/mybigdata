package com.datastory.banyan.monitor;

import com.yeezhao.commons.util.RuntimeUtil;
import org.apache.commons.lang.StringUtils;

/**
 * com.datatub.rhino.monitor.MonConsts
 *
 * Abbreviation keys for saving storage.
 *
 * recommend import: import static com.datatub.rhino.monitor.MonConsts.*;
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public class MonConsts {
    public static final String SEP = ":";
    public static final String OPS_GRP = "ds";
    public static final String OPS_PROJ = "banyan";

    public static final String MON_WBCNT = "wbcontent";
    public static final String MON_WBUSER = "wbuser";
    public static final String MON_ADVUSER = "wbadvuser";
    public static final String MON_WBCMT = "wbcomment";
    public static final String MON_LT = "longtext";
    public static final String MON_WB_UP = "wbupdt";
    public static final String MON_WC_USER = "wcuser";  // wechat user
    public static final String MON_WC_CNT = "wccnt";    // wechat content
    public static final String MON_ECOMM = "ecomm";     // 电商
    public static final String MON_MOVIE = "movie";
    public static final String MON_GAME = "game";
    public static final String MON_BABY = "baby";

    public static final String T = "cost"; // time cost
    public static final String RETRY = "retry"; // retry
    public static final String ANALYZ = "anlyz"; // analyz
    public static final String ALIVE = "alive"; // analyz
    public static final String FILTER = "filter";
    public static final String SUCCESS = "SUCCESS"; // analyz
    public static final String FAIL = "FAIL"; // analyz
    public static final String TOTAL = "TOTAL"; // analyz
    public static final String PERCENT = "percnt"; // analyz
    public static final String M_KAFKA_OUT = "kfk_out";
    public static final String M_ES_IN = "es_in";
    public static final String M_HB_IN = "hb_in";
    public static final String M_ES_OUT = "es_out";
    public static final String M_HB_OUT = "hb_out";
    public static final String M_PH_OUT = "ph_out";


    public static String aliveKey(String name) {
        String sep = SEP;
        return ALIVE + sep + name + sep + RuntimeUtil.getHostname();
    }

    public static String timeKey(String ... keys) {
        String sep = SEP;
        return T + sep + StringUtils.join(keys, sep);
    }

    /**
     * key 按 TYPE + BUSINESS 格式
     * TYPE目前主要指监控的类型：读写数据库？计算？
     * @param arr
     * @return
     */
    public static String keys(String ... arr) {
        String sep = SEP;
        return StringUtils.join(arr, sep);
    }
}
