package com.datastory.banyan.io;

import java.io.Serializable;

/**
 * com.datastory.banyan.io.DataSinkWriteHook
 *
 * @author lhfcws
 * @since 16/11/22
 */
public interface DataSinkWriteHook extends Serializable {
    public void beforeWrite(Object writeRequest);
    public void afterWrite(Object writeRequest, Object writeResponse);
}
