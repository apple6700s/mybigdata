package com.datastory.banyan.utils;

import java.io.Serializable;

/**
 * com.datastory.banyan.utils.ConstructorFunc
 *
 * @author lhfcws
 * @since 2017/4/26
 */
public interface ConstructorFunc<T> extends Serializable {
    T construct();
}
