package com.datastory.banyan.es.hooks;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.async.Function;
import com.datastory.banyan.es.override233.DsBulkProcessor;
import com.datastory.banyan.es.SimpleEsBulkClient;
import com.datastory.banyan.utils.DsRetryable;
import com.datastory.banyan.io.DataSinkWriteHook;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.LinkedList;
import java.util.List;


/**
 * com.datastory.banyan.es.hooks.ESWriteHook
 *
 * @author lhfcws
 * @since 16/11/22
 */
public abstract class ESWriteHook implements DsBulkProcessor.Listener, DataSinkWriteHook, IndexRetry {
    protected static Logger LOG = Logger.getLogger(ESWriteHook.class);
    protected String bulkName;
    protected String index;
    protected String type;
    protected BackoffPolicy indexBackoffPolicy = BackoffPolicy.exponentialBackoff();
    protected SimpleEsBulkClient bulkClient = null;
    protected boolean globalRetryEnable = true;

    public ESWriteHook(String bulkName) {
        this.bulkName = bulkName;
        this.index = bulkName.split("\\.")[0];
        this.type = bulkName.split("\\.")[1];
    }

    public ESWriteHook(SimpleEsBulkClient esBulkClient, String bulkName) {
        this(bulkName);
        this.bulkClient = esBulkClient;
    }

    public ESWriteHook(SimpleEsBulkClient esBulkClient, String bulkName, boolean globalRetryEnable) {
        this(bulkName);
        this.bulkClient = esBulkClient;
        this.globalRetryEnable = globalRetryEnable;
    }

    @Override
    public void beforeBulk(long l, BulkRequest bulkRequest) {
        beforeWrite(bulkRequest);
    }

    @Override
    public void afterBulk(long l, final BulkRequest bulkRequest, final BulkResponse bulkResponse) {
        BulkItemResponse[] responses = bulkResponse.getItems();
        List<ActionRequest> requests = bulkRequest.requests();
        List<ActionRequest> retryList = new LinkedList<>();

        int i = 0;
        for (ActionRequest req : requests) {
            BulkItemResponse response = responses[i];
            boolean isSuccessResponse = isSuccessResponse(response);
            boolean failCanRetry = canRetry(response.getFailure().getCause());
            if (!isSuccessResponse
                    && failCanRetry
                    && globalRetryEnable
                    ) {
                if (req instanceof DsRetryable) {
                    DsRetryable retryable = (DsRetryable) req;
                    retryable.addRetry();
                    if (retryable.canRetry()) {
                        retryList.add(req);
                    }
                }
            } else if (!isSuccessResponse && !failCanRetry) {
                if (req instanceof DsRetryable) {
                    DsRetryable retryable = (DsRetryable) req;
                    retryable.disableRetry();
                }
            }
            i++;
        }

        Async.async(AsyncPool.MONITOR_POOL, new Function() {
            @Override
            public AsyncRet call() throws Exception {
                afterWrite(bulkRequest, bulkResponse);
                return null;
            }
        });

        for (ActionRequest ar : retryList)
            bulkClient.add(ar);
        retryList.clear();
    }

    @Override
    public void afterBulk(long l, final BulkRequest bulkRequest, final Throwable throwable) {
        if (canRetry(throwable)) {
            List<ActionRequest> requests = bulkRequest.requests();

            for (ActionRequest req : requests) {
                DsRetryable retryable = (DsRetryable) req;
                retryable.addRetry();
                if (retryable.canRetry()) {
                    bulkClient.add(req);
                }
            }
            Async.async(AsyncPool.MONITOR_POOL, new Function() {
                @Override
                public AsyncRet call() throws Exception {
                    afterWrite(bulkRequest, throwable);
                    return null;
                }
            });
        } else {
            List<ActionRequest> requests = bulkRequest.requests();

            for (ActionRequest req : requests) {
                DsRetryable retryable = (DsRetryable) req;
                retryable.disableRetry();
            }
            Async.async(AsyncPool.MONITOR_POOL, new Function() {
                @Override
                public AsyncRet call() throws Exception {
                    afterWrite(bulkRequest, throwable);
                    return null;
                }
            });
        }
    }

    public boolean isSuccessResponse(BulkItemResponse res) {
        boolean isSuccess = !res.isFailed();
        isSuccess |= (res.getFailureMessage() != null) &&
                (
                        res.getFailureMessage().contains("DocumentAlreadyExists")
                                || res.getFailure().getCause().getClass().equals(DocumentAlreadyExistsException.class)
                )
        ;
        return isSuccess;
    }

    /**
     * IndexRetry methods
     *
     * @param ir
     * @param listener
     */
    @Deprecated
    public void retry(IndexRequest ir, ActionListener<IndexResponse> listener) {
        if (bulkClient != null) {
            if (listener != null)
                bulkClient.getClient().index(ir, listener);
            else
                bulkClient.getClient().index(ir);
        }
    }

    public boolean canRetry(Throwable e) {
        if (e == null || bulkClient == null) return false;
        boolean flag = e instanceof EsRejectedExecutionException;
        if (!flag && e instanceof RemoteTransportException) {
            flag = e.getMessage().contains(EsRejectedExecutionException.class.getSimpleName());
        }
        return flag;
    }

    public BackoffPolicy getIndexBackoffPolicy() {
        return indexBackoffPolicy;
    }

    public static boolean requestCanRetry(ActionRequest ar) {
        if (ar instanceof DsRetryable) {
            DsRetryable retryable = (DsRetryable) ar;
            return retryable.canRetry();
        } else
            return false;
    }

    public static boolean requestIsRetried(ActionRequest ar) {
        if (ar instanceof DsRetryable) {
            DsRetryable retryable = (DsRetryable) ar;
            return retryable.isRetried();
        } else
            return false;
    }


//    /**
//     * modify response optype to retry (customized optype for hooks)
//     */
//
//    public static Field retryField = null;
//
//    static {
//        try {
//            retryField = BulkItemResponse.class.getDeclaredField("opType");
//        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void markRetry(BulkItemResponse response) {
//        if (retryField != null) {
//            try {
//                retryField.set(response, HookResStatus.RETRY_MARK);
//            } catch (IllegalAccessException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static boolean isMarkedRetry(BulkItemResponse response) {
//        if (response == null) return false;
//        return HookResStatus.RETRY_MARK.equals(response.getOpType());
//    }
}
