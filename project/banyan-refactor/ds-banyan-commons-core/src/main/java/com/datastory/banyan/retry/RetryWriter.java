package com.datastory.banyan.retry;

import java.io.Serializable;

/**
 * com.datastory.banyan.retry.RetryWriter
 *
 * @author lhfcws
 * @since 16/12/8
 */

public interface RetryWriter extends Serializable {
    public void write(String record) throws Exception;

    public void flush() throws Exception;

    public void close();
}
