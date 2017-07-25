package com.datastory.banyan.spark;

/**
 * com.datastory.banyan.spark.MREnum
 *
 * @author lhfcws
 * @since 2017/7/11
 */
public enum MREnum {
    READ, WRITE, FILTER, PASS, ERROR, OTHER,
    MAP, REDUCE, LIST, SHUFFLE, ROWS, EMPTY, NULL,
    SIZE, DELETE, FAIL, SWITCH, DONE, NO_ID, NO_NAME,
    MAP_READ, MAP_WRITE, REDUCE_READ, REDUCE_WRITE,
    ES, HBASE, FILES, DIRS, RECORD
}
