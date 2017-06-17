package com.datastory.banyan.async;

import java.io.Serializable;

/**
 * @author lhfcws
 * @since 16/1/12.
 */
public interface Callback extends Serializable {
    Object callback(Object o);
}
