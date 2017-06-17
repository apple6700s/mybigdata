package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.utils.Args;
import com.datastory.banyan.utils.PatternMatch;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.datastory.banyan.monitor.MonConsts.*;
import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixMonitorHook
 *
 * @author lhfcws
 * @since 16/11/22
 */

public class PhoenixMonitorHook implements DataSinkWriteHook {
    protected static Logger LOG = Logger.getLogger(PhoenixMonitorHook.class);
    protected String table;
    private String totalKey;
    private String successKey;
    private String retryKey;

    public PhoenixMonitorHook(String table) {
        this.table = table;
        this.successKey = keys(M_PH_OUT, table, SUCCESS);
        this.totalKey = keys(M_PH_OUT, table, TOTAL);
        this.retryKey = keys(M_PH_OUT, table, RETRY);
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {

        PatternMatch.create(Args.ArgsList.class, int[].class, new PatternMatch.MatchFunc2<Args.ArgsList, int[]>() {
            @Override
            public void _apply(Args.ArgsList clist, int[] res) {
                if (res == null || res.length == 0) return;
                long success = 0;
                long total = clist.size();
                for (int i = 0; i < res.length; i++) {
                    int r = res[i];
                    if (Status.isJDBCSuccess(r))
                        success++;
                }
                long fail = total - success;
                List<Pair<String, Long>> list = new ArrayList<>();
                list.add(new Pair<>(totalKey, total));
//                    RedisMetricMonitor.getInstance().inc(totalKey, total);
                if (success > 0)
                    list.add(new Pair<>(successKey, success));
//                        RedisMetricMonitor.getInstance().inc(successKey, success);
                if (fail > 0)
                    list.add(new Pair<>(retryKey, fail));
//                        RedisMetricMonitor.getInstance().inc(retryKey, fail);
                RedisMetricMonitor.getInstance().inc(list);
            }
        }).apply(writeRequest, writeResponse);
    }
}
