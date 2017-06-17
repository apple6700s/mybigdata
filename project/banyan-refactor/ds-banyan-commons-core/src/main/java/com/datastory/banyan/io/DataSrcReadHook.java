package com.datastory.banyan.io;

import java.io.Serializable;

/**
 * com.datastory.banyan.io.DataSrcReadHook
 *
 * @author lhfcws
 * @since 16/11/22
 */

public interface DataSrcReadHook extends Serializable {
    public void beforeRead(Object readRequest);
    public void afterRead(Object readRequest, Object readResponse);
}
