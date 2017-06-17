package com.datastory.banyan.spark;

import java.io.Serializable;
import java.util.Map;

/**
 * com.datatub.rhino.spark.SparkRunner
 *
 * @author lhfcws
 * @since 2016/11/9
 */
@Deprecated
public interface SparkRunner extends Serializable {
    public SparkRunner setCores(int cores);
    public int getCores();
    public SparkRunner setSparkConf(Map<String, String> conf);
}
