package com.datastory.banyan.spark;

import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * com.datatub.rhino.spark.AbstractSparkRunner
 *
 * @author lhfcws
 * @since 2016/11/10
 */
public class AbstractSparkRunner implements SparkRunner {
    public static final String SPARK_AKKA_TIMEOUT = "spark.akka.timeout";
    protected int cores = 50;
    protected StrParams sparkConf = new StrParams();

    @Override
    public SparkRunner setCores(int cores) {
        this.cores = cores;
        return this;
    }

    public String getAppName() {
        return this.getClass().getSimpleName();
    }

    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = new StrParams();
        sparkConf.put(SPARK_AKKA_TIMEOUT, "600");
        sparkConf.put("spark.cores.max", "100");
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

    @Override
    public int getCores() {
        return cores;
    }

    @Override
    public SparkRunner setSparkConf(Map<String, String> conf) {
        this.sparkConf = new StrParams(conf);
        return this;
    }

    public JavaSparkContext createSparkContext() {
        return SparkUtil.createSparkContext(this.getClass(), false, this.getClass().getSimpleName(), cores + "", sparkConf);
    }
}
