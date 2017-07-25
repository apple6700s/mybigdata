package com.datastory.banyan.weibo.abel;

import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * com.datastory.banyan.hbase.HBaseUtils
 *
 * @author sezina
 * @since 7/17/16
 */
public class HBaseUtils {
    public static Map<String, String> getFamilyMap(Result result, byte[] family) {
        Map<byte[], byte[]> bm = result.getFamilyMap(family);
        if (bm != null) {
            return BanyanTypeUtil.byte2strMap(bm);
        } else
            return null;
    }

    public static String getValue(Result result, byte[] family, byte[] qualify) {
        byte[] b = result.getValue(family, qualify);
        if (b == null || b.length == 0) return null;
        return Bytes.toString(b);
    }

    public static Map<String, String> getValues(Result result, byte[] family) {
        Map<byte[], byte[]> bm = result.getFamilyMap(family);
        Map<String, String> mp = BanyanTypeUtil.byte2strMap(bm);
        return mp;
    }

    public static Scan buildScan() {
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        return scan;
    }

    public static Scan buildScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        if (startRow != null)
            scan.setStartRow(startRow.getBytes());
        if (stopRow != null)
            scan.setStopRow(stopRow.getBytes());
        return scan;
    }


    public static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * HBase wbuserPK key
     *
     * @param uid
     * @return
     */
    public static String wbuserPK(String uid) {
        return Md5Util.md5(uid).substring(0, 2) + uid;
    }

    public static String wbcontentPK(String mid) {
        return Md5Util.md5(mid).substring(0, 3) + mid;
    }

    public static String advUserPK(String updateDate, String uid) {
        return updateDate + "|" + uid;
    }

}
