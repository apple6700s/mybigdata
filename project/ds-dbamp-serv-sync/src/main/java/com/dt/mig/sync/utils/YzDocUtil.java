package com.dt.mig.sync.utils;

import com.ds.dbamp.core.dao.es.YZDoc;

/**
 * Created by abel.chan on 17/5/16.
 */
public class YzDocUtil {
    public static boolean isExistAndNotEmpty(YZDoc yzDoc, String field) {
        if (yzDoc.containsKey(field) && yzDoc.get(field) != null) {
            return true;
        }
        return false;
    }
}
