package com.datastory.banyan.es.hooks;

import com.datastory.banyan.retry.record.HDFSLog;
import com.datastory.banyan.utils.PatternMatch;
import com.yeezhao.commons.util.StringUtil;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * com.datastory.banyan.es.hooks.ESRetryLogHook
 *
 * @author lhfcws
 * @since 16/12/8
 */
@Deprecated
public class ESRetryLogHook extends ESWriteHook {
    static Class<IndexRequest> klass = IndexRequest.class;
    static Field sourceField = null;

    static {
        try {
            sourceField = klass.getDeclaredField("source");
            sourceField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            System.err.println("ES SDK 版本不兼容，请检查 class IndexRequest 有无 source 字段。");
            e.printStackTrace();
        }
    }


    public ESRetryLogHook(String bulkName) {
        super(bulkName);
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {
        if (sourceField == null) return;

        PatternMatch.create(BulkRequest.class, BulkResponse.class, new PatternMatch.MatchFunc2<BulkRequest, BulkResponse>() {
            @Override
            public void _apply(BulkRequest request, BulkResponse response) {
                List<ActionRequest> requests = request.requests();
                HDFSLog hdfsLog = null;

                Iterator<BulkItemResponse> iter = response.iterator();
                int i = 0;

                while (iter.hasNext()) {
                    BulkItemResponse res = iter.next();
                    ActionRequest req = requests.get(i);
                    i++;
                    if (isSuccessResponse(res) || requestCanRetry(req)) continue;

                    if (req instanceof IndexRequest) {
                        IndexRequest ir = (IndexRequest) req;
                        try {
                            BytesReference bytesReference = (BytesReference) sourceField.get(ir);
                            String source = bytesReference.toUtf8();
                            if (source != null)
                                source = source.trim();
                            if (StringUtil.isNullOrEmpty(source))
                                continue;

                            if (hdfsLog == null)
                                hdfsLog = HDFSLog.get("es." + bulkName);
                            hdfsLog.log(source);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(BulkRequest.class, Throwable.class, new PatternMatch.MatchFunc2<BulkRequest, Throwable>() {
            @Override
            public void _apply(BulkRequest request, Throwable throwable) {
                List<ActionRequest> requests = new ArrayList<>(request.requests());
                HDFSLog hdfsLog = null;
                for (ActionRequest req : requests) {
                    if (requestCanRetry(req)) continue;

                    if (req instanceof IndexRequest) {
                        IndexRequest ir = (IndexRequest) req;
                        try {
                            BytesReference bytesReference = (BytesReference) sourceField.get(ir);
                            String source = bytesReference.toUtf8();
                            if (source != null)
                                source = source.trim();
                            if (StringUtil.isNullOrEmpty(source))
                                continue;
                            if (hdfsLog == null)
                                hdfsLog = HDFSLog.get("es." + bulkName);
                            hdfsLog.log(source);
                        } catch (IllegalAccessException e) {
                            LOG.error(e);
                        }
                    }
                }
                if (hdfsLog != null) {
                    try {
                        hdfsLog.flush();
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

//        PatternMatch.create(IndexRequest.class, Throwable.class, new PatternMatch.MatchFunc2<IndexRequest, Throwable>() {
//            @Override
//            public void _apply(IndexRequest indexRequest, Throwable throwable) {
//                HDFSLog hdfsLog = null;
//                try {
//                    BytesReference bytesReference = (BytesReference) sourceField.get(indexRequest);
//                    String source = bytesReference.toUtf8();
//                    if (source != null)
//                        source = source.trim();
//                    if (StringUtil.isNullOrEmpty(source))
//                        return;
//                    hdfsLog = HDFSLog.get("es." + bulkName);
//                    hdfsLog.log(source);
//                    hdfsLog.flush();
//                } catch (Exception e) {
//                    LOG.error(e);
//                }
//            }
//        }).apply(writeRequest, writeResponse);
    }
}
