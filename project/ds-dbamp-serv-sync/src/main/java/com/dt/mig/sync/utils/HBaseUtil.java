package com.dt.mig.sync.utils;

import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;


public class HBaseUtil {

    public static String getValue(Result result, byte[] family, byte[] qualify) {
        byte[] b = result.getValue(family, qualify);
        if (b == null) return null;
        return Bytes.toString(b);
    }

    public static Map<String, String> getFamilyMap(Result result, byte[] family) {
        Map<byte[], byte[]> bm = result.getFamilyMap(family);
        if (bm != null) {
            return byte2strMap(bm);
        } else return null;
    }

    public static StrParams byte2strMap(Map<byte[], byte[]> mp) {
        StrParams strParams = new StrParams();
        for (Map.Entry<byte[], byte[]> e : mp.entrySet()) {
            if (e.getValue() != null) strParams.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
        }
        return strParams;
    }
}
