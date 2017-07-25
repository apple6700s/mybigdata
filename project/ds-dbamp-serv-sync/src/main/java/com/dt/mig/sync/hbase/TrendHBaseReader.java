package com.dt.mig.sync.hbase;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.utils.HBaseUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

/**
 * com.dt.mig.sync.hbase.TrendHBaseReader
 *
 * @author lhfcws
 * @since 2017/4/5
 */
public class TrendHBaseReader {

    public Params readTrend(String pk, int latestCnt) throws IOException {
        Get get = new Get(pk.getBytes());
        return readTrend(get, latestCnt);
    }

    public Params readTrend(Get get, int latestCnt) throws IOException {
        if (latestCnt <= 0) get = get.setMaxVersions();
        else get = get.setMaxVersions(latestCnt);

        HTableInterface hti = HTableInterfacePool.get(MigSyncConsts.HBASE_TREND_TBL_NEW);
        try {
            Result result = hti.get(get);
            if (result == null || result.isEmpty()) return null;

            Params out = new Params();
            out.put("source", HBaseUtil.getValue(result, "r".getBytes(), "source".getBytes()));
            out.put("id", HBaseUtil.getValue(result, "r".getBytes(), "id".getBytes()));
            out.put("type", HBaseUtil.getValue(result, "r".getBytes(), "type".getBytes()));

            Map<Long, byte[]> datas = result.getMap().get("r".getBytes()).get("data".getBytes());
            if (datas != null && !datas.isEmpty()) out.put("data", datas);

            for (Map.Entry<Long, byte[]> entry : datas.entrySet()) {
                System.out.println("date:" + entry.getKey() + ":::" + "data:" + new String(entry.getValue()));
            }

            return out;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            hti.close();
        }
    }

    public static void main(String[] args) {
        try {
            TrendHBaseReader trendHBaseReader = new TrendHBaseReader();
            Params params = trendHBaseReader.readTrend(HBaseUtils.wbTrendPK("4106200347813086"), 30);
            System.out.println(params.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
