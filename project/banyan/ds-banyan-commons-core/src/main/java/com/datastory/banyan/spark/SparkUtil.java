package com.datastory.banyan.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.monitor.daemon.ReceiverKill;
import com.datastory.commons.spark.client.SparkYarnClusterSubmit;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.Triple;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaRDD;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by neo on 15-1-9.
 */
public class SparkUtil {
    private static final Logger LOG = Logger.getLogger(SparkUtil.class);

    public static final String PARAM_SPARK_MASTER_URL = "spark.master.url";
    // parallelism
    public static final String SPARK_AKKA_TIMEOUT = "spark.akka.timeout";

    public static JavaSparkContext createSparkContext(Class<?> jars, String appName, int cores) {
        return createSparkContext(jars, false, appName, cores + "", null);
    }

    public static JavaSparkContext createSparkContext(Class<?> jars, String appName, int cores, Map<String, String> confParams) {
        return createSparkContext(jars, false, appName, cores + "", confParams);
    }

    public static JavaSparkContext createSparkContext(Class<?> jars, boolean isLocalMode, String appName, String coreNum) {
        return createSparkContext(jars, isLocalMode, appName, coreNum, null);
    }

    public static JavaSparkContext createSparkContext(Class<?> jars, boolean isLocalMode, String appName, String coreNum, Map<String, String> confParams) {
        RhinoETLConfig conf = RhinoETLConfig.getInstance();
        String masterUrl = conf.get(PARAM_SPARK_MASTER_URL);
        //  masterUrl = "local[3]";
        if (isLocalMode) {
            masterUrl = "local[*]";
        }
        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl)
                .setAppName(appName)
                .setJars(JavaSparkContext.jarOfClass(jars))
                .set(SPARK_AKKA_TIMEOUT, "600")
                .set("spark.cores.max", coreNum)
                .set("spark.default.parallelism", coreNum)
                .set("spark.executor.memory", "2048m")
                .set("spark.speculation.multiplier", "1.0")
                .set("spark.streaming.backpressure.enabled", "true")
//                .set("spark.executor.userClassPathFirst", "true")
                .set("spark.executor.logs.rolling.strategy", "size")
                .set("spark.executor.logs.rolling.maxRetainedFiles", "10")
                .set("spark.executor.logs.rolling.size.maxBytes", "134217728") // 128m
                ;
        String sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.executor.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.executor.extraJavaOptions", sparkExecutorOpts + " -Dlog4j.configuration=file:/opt/package/spark/conf/log4j.simple.properties");
        sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.driver.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.driver.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC");

        if (confParams != null)
            for (Map.Entry<String, String> e : confParams.entrySet())
                sparkConf.set(e.getKey(), e.getValue());

        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            sparkConf.setSparkHome(sparkHome);
        }
        return new JavaSparkContext(sparkConf);
    }

    @Deprecated
    public static JavaSparkContext createYarnSparkContext(Map<String, String> confParams) {
        SparkConf sparkConf = SparkYarnClusterSubmit.getLocalOrNewSparkConf();
        sparkConf
                .set(SPARK_AKKA_TIMEOUT, "600")
                .set("spark.executor.memory", "2048m")
                .set("spark.speculation.multiplier", "1.0")
                .set("spark.streaming.backpressure.enabled", "true")
//                .set("spark.executor.userClassPathFirst", "true")
                .set("spark.executor.logs.rolling.strategy", "size")
                .set("spark.executor.logs.rolling.maxRetainedFiles", "10")
                .set("spark.executor.logs.rolling.size.maxBytes", "134217728") // 128m
        ;

        String sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.executor.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.executor.extraJavaOptions", sparkExecutorOpts + " -Dlog4j.configuration=file:/opt/package/spark/conf/log4j.simple.properties");
        sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.driver.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.driver.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC");

        if (confParams != null)
            for (Map.Entry<String, String> e : confParams.entrySet())
                sparkConf.set(e.getKey(), e.getValue());

        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            sparkConf.setSparkHome(sparkHome);
        }
        return new JavaSparkContext(sparkConf);
    }

    public static JavaSparkContext createSparkContext(Class<?> jarOfClass, boolean isLocalMode, String appName) {
        return SparkUtil.createSparkContext(jarOfClass, isLocalMode, appName, "20");
    }

    public static Triple<Integer, Long, Long> getKafkaOffset(JavaPairRDD<String, String> rdd) {
        OffsetRange[] offsetRanges = ((KafkaRDD<?, ?, ?, ?, ?>) rdd.rdd()).offsetRanges();
        if (offsetRanges.length > 0) {
            OffsetRange offsetRange = offsetRanges[0];
            return new Triple<>(offsetRange.partition(), offsetRange.fromOffset(), offsetRange.untilOffset());
        } else
            return null;
    }

    public static void stopStreamingContext(JavaStreamingContext jssc, long batchIntervalSeconds, String appName) {
        if (jssc != null) {
            jssc.ssc().scheduler().receiverTracker().stop(true);
            LOG.info("[stopStreamingContext] Stopped receiverTracker.");
            try {
                Thread.sleep(batchIntervalSeconds * 1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }

            if (appName == null)
                appName = jssc.sc().getConf().get("spark.app.name");
            try {
                new ReceiverKill().kill(appName);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("[stopStreamingContext] Stopped receiving task. " + appName);

            jssc.stop(true, true);
            jssc.close();
            LOG.info("[stopStreamingContext] Stopped SparkContext.");
        }
    }

    public static void stopStreamingContext(String appName, String pid, long batchIntervalSeconds) throws Exception {
        RuntimeUtil.killProcess(pid);
        LOG.info("kill " + pid);

        try {
            Thread.sleep(batchIntervalSeconds * 1000);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        boolean exists = RuntimeUtil.existProcess(pid);
        if (exists) {
            try {
                new ReceiverKill().kill(appName);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("Stopped receiver task. " + appName);
        }
    }

    public static void stopStreamingContextForce(String appName, String pid, long batchIntervalSeconds) throws Exception {
        RuntimeUtil.killProcess(pid);
        LOG.info("kill " + pid);

        try {
            Thread.sleep(batchIntervalSeconds * 1000);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        boolean exists = RuntimeUtil.existProcess(pid);
        if (exists) {
            try {
                new ReceiverKill().kill(appName);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("Stopped receiver task. " + appName);
        }

        try {
            Thread.sleep(batchIntervalSeconds * 1000);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        exists = RuntimeUtil.existProcess(pid);
        if (exists) {
            RuntimeUtil.killProcess(pid, true);
            LOG.info("kill -9 " + pid);
        }
    }
}
