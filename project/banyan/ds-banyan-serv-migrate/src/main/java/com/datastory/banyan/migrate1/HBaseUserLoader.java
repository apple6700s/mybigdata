package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.HBaseCSV;
import com.datastory.banyan.weibo.hbase.WbUserHBaseReader;
import com.yeezhao.commons.util.Entity.Params;

import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * com.datastory.banyan.migrate1.HBaseUserLoader
 *
 * @author lhfcws
 * @since 2017/1/10
 */
public class HBaseUserLoader {
    public void save(String cntInputCsv, String userOutputCsv) throws Exception {
        HBaseCSV csv1 = HBaseCSV.load(Tables.table(Tables.PH_WBCNT_TBL), cntInputCsv);
        HashSet<String> uids = new HashSet<>();
        while (csv1.hasNext()) {
            Params p = csv1.next();
            if (p == null)
                 continue;
            String uid = p.getString("uid");
            if (uid != null)
                uids.add(uid);
        }
        csv1.close();

        System.out.println("User size : " + uids.size());

        HBaseCSV csv2 = new HBaseCSV(Tables.table(Tables.PH_WBUSER_TBL), userOutputCsv);
        csv2.beginSave();
        WbUserHBaseReader reader = WbUserHBaseReader.getInstance();
        for (String uid : uids) {
            String pk = BanyanTypeUtil.wbuserPK(uid);
            List<Params> ret = reader.batchRead(pk);
            if (ret != null) {
                for (Params p : ret)
                    csv2.save(p);
            }
        }
        List<Params> ret = reader.flush();
        if (ret != null) {
            for (Params p : ret)
                csv2.save(p);
        }
        csv2.endSave();
        csv2.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        HBaseUserLoader loader = new HBaseUserLoader();
        loader.save(args[0], args[1]);
        System.out.println("[PROGRAM] Program exited.");
    }
}
