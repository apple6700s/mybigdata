package com.datastory.banyan.migrate1;

import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.utils.HBaseCSV;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * com.datastory.banyan.migrate1.HBaseCSVExporter
 * export hbase to csv
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class HBaseCSVExporter {

    public void exportCsv(String table, String csvFile, int limit) throws IOException {
        HBaseCSV csv = new HBaseCSV(table, csvFile);
        csv.beginSave();
        Scan scan = HBaseUtils.buildScan();
//        SingleColumnValueFilter filter = new SingleColumnValueFilter("r".getBytes(), "publish_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, "20170101000000".getBytes());
//        scan.setFilter(filter);
        HTableInterface hti = null;
        try {
            hti = HTableInterfacePool.get(table);
            ResultScanner scanner = hti.getScanner(scan);
            long cnt = 0;

            LOOP:
            while (cnt < limit || limit <= 0) {
                Result[] results = scanner.next(100);
                if (results == null || results.length == 0) break;
                for (int i = 0; i < results.length; i++) {
                    cnt++;
                    if (cnt >= limit && limit > 0)
                        break LOOP;

                    Params p = new ResultRDocMapper(results[i]).map();
                    if (p != null) {
                        Params p1 = new Params();
                        for (Map.Entry<String, Object> e : p.entrySet()) {
                            if (e.getValue() instanceof String) {
                                String v = (String) e.getValue();
                                p1.put(e.getKey(), v);
                            } else
                                p1.put(e.getKey(), e.getValue());
                        }
                        csv.save(p1);
                    }
                }
            }
            csv.endSave();

            System.out.println("Got records size : " + cnt);
        } finally {
            if (hti != null) {
                hti.close();
            }
            csv.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        HBaseCSVExporter exporter = new HBaseCSVExporter();
        int limit = 0;
        if (args.length > 2)
            limit = Integer.valueOf(args[2]);
        exporter.exportCsv(args[0], args[1], limit);
        System.out.println("[PROGRAM] Program exited.");
    }
}
