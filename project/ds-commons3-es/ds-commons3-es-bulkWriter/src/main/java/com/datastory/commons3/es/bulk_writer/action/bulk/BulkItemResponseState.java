package com.datastory.commons3.es.bulk_writer.action.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.mapper.MapperParsingException;

public enum BulkItemResponseState {
    OK,
    DATA_WRONG,
    ALREADY_EXISTS,
    REQUEST_FAIL;

    public static BulkItemResponseState getState(BulkItemResponse.Failure failure) {
        if (failure == null) {
            return OK;
        }
        Throwable t = failure.getCause();
        if (t instanceof MapperParsingException) {
            return DATA_WRONG;
        } else if (t instanceof DocumentAlreadyExistsException) {
            return ALREADY_EXISTS;
        }
        return REQUEST_FAIL;
    }

    public static boolean isBulkResponseSuccess(BulkItemResponse res) {
        boolean isSuccess = !res.isFailed();
        isSuccess |= (res.getFailureMessage() != null) &&
                (
                        res.getFailureMessage().contains("DocumentAlreadyExists")
                                || res.getFailure().getCause().getClass().equals(DocumentAlreadyExistsException.class)
                )
        ;
        return isSuccess;
    }
}
