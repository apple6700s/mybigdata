package com.datastory.banyan.async;

import java.io.Serializable;

/**
 * @author lhfcws
 * @since 16/1/12.
 */
public interface Function extends Serializable {
    public AsyncRet call() throws Exception;
}
