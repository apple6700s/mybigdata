package com.datastory.banyan.req;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.transport.RemoteTransportException;

import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * com.datastory.banyan.req.AckUtil
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class AckUtil {
    public static boolean isJDBCSuccess(int r) {
        return r >= 0 || r == SUCCESS_NO_INFO;
    }

    public static boolean isHBaseSuccess(int r) {
        return r > 0;
    }

    public static boolean isBulkResponseSuccess(BulkItemResponse res) {
        boolean isSuccess = !res.isFailed();
        isSuccess |= (res.getFailureMessage() != null) && isDocumentExist(res);
        return isSuccess;
    }

    public static boolean isDocumentExist(BulkItemResponse res) {
        return (
                res.getFailureMessage().contains("DocumentAlreadyExists")
                        || res.getFailure().getCause().getClass().equals(DocumentAlreadyExistsException.class)
        );
    }

    public static boolean canBulkRetry(BulkItemResponse r) {
        return canBulkRetry(r.getFailure().getCause()) || canBulkRetry(r.getFailureMessage());
    }

    public static boolean canBulkRetry(Throwable e) {
        if (e == null) return false;
        boolean flag = e instanceof EsRejectedExecutionException;
        if (!flag && e instanceof RemoteTransportException) {
            flag = e.getMessage().contains(EsRejectedExecutionException.class.getSimpleName());
        }
        return flag;
    }

    public static boolean canBulkRetry(String msg) {
        return msg != null && msg.contains(EsRejectedExecutionException.class.getSimpleName());
    }
}
