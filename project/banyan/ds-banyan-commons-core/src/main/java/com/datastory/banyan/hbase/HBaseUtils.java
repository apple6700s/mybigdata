package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    public static Scan buildDateScan(String prefix, Date d) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        SimpleDateFormat sdf = new SimpleDateFormat(RhinoETLConsts.DFT_DAYFORMAT);
        String startDate = prefix + sdf.format(calendar.getTime());
        calendar.add(Calendar.DATE, +1);
        String endDate = prefix + sdf.format(calendar.getTime());

        return buildScan(startDate, endDate);
    }

    public static Scan buildStartScan(String prefix, Date d) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        SimpleDateFormat sdf = new SimpleDateFormat(RhinoETLConsts.DFT_TIMEFORMAT);
        String startDate = prefix + sdf.format(calendar.getTime());
        Scan scan = buildScan();
        scan.setStartRow(startDate.getBytes());
        return scan;
    }

    public static long scanNDelete(String table, Scan scan, int batch) throws IOException {
        HTableInterface hTable = HTableInterfacePool.get(table);
        ResultScanner scanner = hTable.getScanner(scan);

        List<Delete> cache = new LinkedList<>();
        int delSize = 10000;
        long cnt = 0;
        try {
            while (true) {
                Result[] results = scanner.next(batch);
                if (results == null || results.length == 0) break;
                cnt += results.length;
                for (Result result : results) {
                    Delete delete = new Delete(result.getRow());
                    cache.add(delete);
                }

                if (cache.size() > delSize) {
                    hTable.delete(cache);
                    cache.clear();
                }
            }

            if (cache.size() > 0) {
                hTable.delete(cache);
                cache.clear();
            }
        } finally {
            hTable.close();
        }

        return cnt;
    }

    public static String currentUpdateDate() {
        SimpleDateFormat sdf = new SimpleDateFormat(RhinoETLConsts.DFT_TIMEFORMAT);
        return sdf.format(new Date());
    }

    public static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * 趋势写入
     *
     * @param putter
     * @param p
     * @return
     * @throws Exception
     */
    public static int writeTrend(RFieldPutter putter, Params p) throws Exception {
        String d = (String) p.get("update_date");
        if (!BanyanTypeUtil.valid(d))
            return -1;
        Date date = DateUtils.parse(d, DateUtils.DFT_TIMEFORMAT);
        date.setMinutes(0);
        date.setSeconds(0);
        long ts = date.getTime() / 1000;
        ts *= 1000;

        return putter.batchWrite(p, ts);
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

    public static class PutList extends ArrayList<Put> {
        public PutList() {
        }

        public PutList(Collection<? extends Put> c) {
            super(c);
        }
    }
}
