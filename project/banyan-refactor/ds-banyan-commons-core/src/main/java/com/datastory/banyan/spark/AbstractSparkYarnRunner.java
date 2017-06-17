package com.datastory.banyan.spark;

import com.yeezhao.commons.util.Entity.StrParams;

import java.util.NoSuchElementException;

/**
 * com.datastory.banyan.spark.AbstractSparkYarnRunner
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public abstract class AbstractSparkYarnRunner implements SparkYarnRunner {
    public static final String SPARK_AKKA_TIMEOUT = "spark.akka.timeout";

    @Override
    public String getAppName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getYarnQueue() {
        return "default";
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = new StrParams();
        sparkConf.put(SPARK_AKKA_TIMEOUT, "600");
        sparkConf.put("spark.cores.max", "100");
        sparkConf.put("spark.executor.cores", "2");
        sparkConf.put("spark.executor.instances", "50");
        sparkConf.put("spark.default.parallelism", "100");
        sparkConf.put("spark.executor.memory", "2500m");
        sparkConf.put("spark.speculation.multiplier", "1.5");
        sparkConf.put("spark.streaming.backpressure.enabled", "true");
        sparkConf.put("spark.executor.logs.rolling.strategy", "size");
        sparkConf.put("spark.executor.logs.rolling.maxRetainedFiles", "10");
        sparkConf.put("spark.executor.logs.rolling.size.maxBytes", "134217728"); // 128m
        String sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.executor.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.put("spark.executor.extraJavaOptions", sparkExecutorOpts + " -Dlog4j.configuration=file:/opt/package/spark/conf/log4j.simple.properties");
        sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.driver.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.put("spark.driver.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC");
        return sparkConf;
    }
}
