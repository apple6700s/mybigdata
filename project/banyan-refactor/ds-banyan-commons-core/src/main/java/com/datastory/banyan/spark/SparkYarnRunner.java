package com.datastory.banyan.spark;

import com.yeezhao.commons.util.Entity.StrParams;

import java.io.Serializable;

/**
 * com.datastory.banyan.spark.SparkYarnRunner
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public interface SparkYarnRunner extends Serializable {
    public String getAppName();
    public String getYarnQueue();
    public StrParams customizedSparkConfParams();
}
