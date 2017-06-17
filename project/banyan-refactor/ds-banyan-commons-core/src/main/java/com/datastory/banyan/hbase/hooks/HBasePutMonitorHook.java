package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.utils.PatternMatch;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.datastory.banyan.monitor.MonConsts.*;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixMonitorHook
 *
 * @author lhfcws
 * @since 16/11/22
 */

public class HBasePutMonitorHook implements DataSinkWriteHook {
    protected static Logger LOG = Logger.getLogger(HBasePutMonitorHook.class);
    protected String table;
    private String totalKey;
    private String successKey;
    private String retryKey;

    public HBasePutMonitorHook(String table) {
        this.table = table;
        this.successKey = keys(M_HB_OUT, table, SUCCESS);
        this.totalKey = keys(M_HB_OUT, table, TOTAL);
        this.retryKey = keys(M_HB_OUT, table, RETRY);
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {

        PatternMatch.create(List.class, Integer.class, new PatternMatch.MatchFunc2<List, Integer>() {
            @Override
            public void _apply(List clist, Integer res) {
                long total = clist.size();
                if (total > 0) {
                    long success = 0;
                    long fail = 0;
                    if (Status.isHBaseSuccess(res)) {
                        success = total;
                    } else {
                        fail = total;
                    }

                    List<Pair<String, Long>> list = new ArrayList<>();
                    if (total > 0)
                        list.add(new Pair<>(totalKey, total));
                    if (success > 0)
                        list.add(new Pair<>(successKey, success));
                    if (fail > 0)
                        list.add(new Pair<>(retryKey, fail));
                    RedisMetricMonitor.getInstance().inc(list);
                }
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(List.class, int[].class, new PatternMatch.MatchFunc2<List, int[]>() {
            @Override
            public void _apply(List clist, int[] reses) {
                if (clist == null || reses == null || clist.size() == 0 || reses.length == 0) return;
                int res = reses[0];
                int total = clist.size();
                if (total > 0) {
                    int success = 0;
                    int fail = 0;
                    if (Status.isHBaseSuccess(res)) {
                        success = total;
                    } else {
                        fail = total;
                    }

                    List<Pair<String, Long>> list = new ArrayList<>();
                    if (total > 0)
                        list.add(new Pair<>(totalKey, (long) total));
                    if (success > 0)
                        list.add(new Pair<>(successKey, (long) success));
                    if (fail > 0)
                        list.add(new Pair<>(retryKey, (long) fail));

                    RedisMetricMonitor.getInstance().inc(list);
                }
            }
        }).apply(writeRequest, writeResponse);
    }
}
