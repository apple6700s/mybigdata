package com.datastory.banyan.es.hooks;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.banyan.utils.PatternMatch;
import org.apache.hadoop.hbase.util.Pair;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;
import java.util.List;

import static com.datastory.banyan.monitor.MonConsts.*;

/**
 * com.datastory.banyan.es.hooks.EsMonitorHook
 *
 * @author lhfcws
 * @since 16/11/22
 */
@Deprecated
public class EsMonitorHook extends ESWriteHook {
    final String totalKey = MonConsts.keys(M_ES_OUT, bulkName, TOTAL);
    final String successKey = MonConsts.keys(M_ES_OUT, bulkName, SUCCESS);
    final String retryKey = MonConsts.keys(M_ES_OUT, bulkName, RETRY);
    final String failKey = MonConsts.keys(M_ES_OUT, bulkName, FAIL);

    public EsMonitorHook(String bulkName) {
        super(bulkName);
    }

    @Override
    public void beforeWrite(Object writeRequest) {
        BulkRequest request = (BulkRequest) writeRequest;
        LOG.info("[BEFORE] " + bulkName + " actions:" + request.numberOfActions() + ", size : " + request.estimatedSizeInBytes());
    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {

        PatternMatch.create(BulkRequest.class, BulkResponse.class, new PatternMatch.MatchFunc2<BulkRequest, BulkResponse>() {
            @Override
            public void _apply(BulkRequest request, BulkResponse response) {
                long successCnt = 0;
                long totalCnt = 0;
                long retryCnt = 0;
                long failCnt = 0;

                String sampleError = null;
                List<ActionRequest> requests = request.requests();  // ArrayList

                int i = 0;
                for (BulkItemResponse res : response) {
                    if (isSuccessResponse(res)) {
                        successCnt++;
                    } else if (requestCanRetry(requests.get(i))) {
                        retryCnt++;
                    } else {
                        failCnt++;
                        sampleError = res.getFailureMessage() + " - " + ErrorUtil.e2s(res.getFailure().getCause());
                    }
                    i++;
                }
                totalCnt = i;

                List<Pair<String, Long>> list = new ArrayList<>();
                if (retryCnt > 0)
                    list.add(new Pair<>(retryKey, retryCnt));
//                    RedisMetricMonitor.getInstance().inc(retryKey, retryCnt);
                if (failCnt > 0)
                    list.add(new Pair<>(failKey, failCnt));
//                    RedisMetricMonitor.getInstance().inc(failKey, failCnt);
                if (successCnt > 0)
                    list.add(new Pair<>(successKey, successCnt));
//                    RedisMetricMonitor.getInstance().inc(successKey, successCnt);
                if (totalCnt > 0)
                    list.add(new Pair<>(totalKey, totalCnt));
//                    RedisMetricMonitor.getInstance().inc(totalKey, totalCnt);
                if (!list.isEmpty())
                    RedisMetricMonitor.getInstance().inc(list);

                if (retryCnt > 0 || successCnt < totalCnt)
                    LOG.error("[AFTER] " + bulkName + " actions:" + totalCnt + ", success : " + successCnt + ", fail : " + failCnt + ", retry : " + retryCnt);
                if (failCnt > 0)
                    LOG.error("[AFTER] " + bulkName + " partly fail | Exception: " + sampleError);
                LOG.info("[AFTER] " + bulkName + " actions:" + totalCnt + ", success : " + successCnt + ", fail : " + failCnt + ", retry : " + retryCnt);
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(BulkRequest.class, Throwable.class, new PatternMatch.MatchFunc2<BulkRequest, Throwable>() {
            @Override
            public void _apply(BulkRequest request, Throwable throwable) {
                long total = 0;
                for (ActionRequest ar : request.requests()) {
                    if (!requestCanRetry(ar))
                        total++;
                }
                if (total > 0)
                    RedisMetricMonitor.getInstance().inc(totalKey, total);
                LOG.error("[AFTER] " + bulkName + " actions:" + request.numberOfActions() + ", Exception : " + ErrorUtil.e2s(throwable));
                LOG.info("[AFTER] " + bulkName + " actions:" + request.numberOfActions() + ", Exception : " + ErrorUtil.e2s(throwable));
            }
        }).apply(writeRequest, writeResponse);

//        PatternMatch.create(IndexRequest.class, Throwable.class, new PatternMatch.MatchFunc2<IndexRequest, Throwable>() {
//            @Override
//            public void _apply(IndexRequest indexRequest, Throwable t) {
//                RedisMetricMonitor.getInstance().inc(totalKey, 1);
//                LOG.info("[AFTER] " + bulkName + " index actions: 1 , Exception : " + ErrorUtil.e2s(t));
//                LOG.error("[AFTER] " + bulkName + " index actions: 1 , Exception : " + ErrorUtil.e2s(t));
//            }
//        }).apply(writeRequest, writeResponse);
//
//        PatternMatch.create(IndexRequest.class, Integer.class, new PatternMatch.MatchFunc2<IndexRequest, Integer>() {
//            @Override
//            public void _apply(IndexRequest indexRequest, final Integer i) {
//                final Reference<Integer> total = new Reference<Integer>(1);
//                final Reference<Integer> success = new Reference<Integer>(1);
//                if (i == -1) {
//                    success.setValue(0);
//                }
//
//                if (total.getValue() > 0)
//                    RedisMetricMonitor.getInstance().inc(totalKey, total.getValue());
//                if (success.getValue() > 0) {
//                    RedisMetricMonitor.getInstance().inc(successKey, success.getValue());
//                    LOG.info("[AFTER] " + bulkName + " no retry actions: " + total + " , success : " + success);
//                } else
//                    LOG.error("[AFTER] " + bulkName + " no retry actions: " + total + " , success : " + success);
//            }
//        }).apply(writeRequest, writeResponse);
    }
}
