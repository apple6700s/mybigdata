package com.datastory.banyan.migrate1;

import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.HBaseCSV;
import com.yeezhao.commons.util.Entity.Params;

import java.lang.management.ManagementFactory;

/**
 * com.datastory.banyan.migrate1.HBaseCSVImporter
 * import csv to hbase
 * @author lhfcws
 * @since 2017/1/10
 */
public class HBaseCSVImporter {
    public void importCsv(final String table, String inputCsv) throws Exception {
        HBaseCSV hBaseCSV = HBaseCSV.load(table, inputCsv);
        RFieldPutter putter = new RFieldPutter(table);

        while (hBaseCSV.hasNext()) {
            Params p = hBaseCSV.next();
            if (p != null)
                putter.batchWrite(p);
        }
        putter.flush();
        hBaseCSV.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        HBaseCSVImporter loader = new HBaseCSVImporter();
        loader.importCsv(args[0], args[1]);
        System.exit(0);
        System.out.println("[PROGRAM] Program exited.");
    }
}
