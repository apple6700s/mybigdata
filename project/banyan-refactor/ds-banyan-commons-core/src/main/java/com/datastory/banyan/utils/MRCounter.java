package com.datastory.banyan.utils;

/**
 * com.datastory.banyan.utils.MRCounter
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public enum MRCounter {
    READ, WRITE, FILTER, PASS, ERROR, OTHER,
    MAP, REDUCE, LIST, SHUFFLE, ROWS, EMPTY, NULL,
    SIZE, DELETE
}
