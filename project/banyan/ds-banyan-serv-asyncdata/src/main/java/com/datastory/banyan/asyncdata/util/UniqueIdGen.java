package com.datastory.banyan.asyncdata.util;

import com.yeezhao.commons.util.encypt.Md5Util;

/**
 * com.datastory.banyan.asyncdata.util.UniqueIdGen
 *
 * @author lhfcws
 * @since 2017/4/20
 */
public class UniqueIdGen {

    public static String gen(String siteId, String itemId) {
        return Md5Util.md5(siteId + "_" + itemId);
    }

    public static String gen(Object obj) {
        return Md5Util.md5(obj + "") + System.currentTimeMillis();
    }
}
