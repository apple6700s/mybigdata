package com.dt.mig.sync.utils;

import com.datastory.commons.spark.client.SparkYarnConf;
import com.dt.mig.sync.base.MigSyncConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by neo on 15-1-9.
 */
public class SparkUtil {
    public static final String PARAM_SPARK_MASTER_URL = "spark.master.url";
    public static final String SPARK_AKKA_TIMEOUT = "spark.akka.timeout";

    /**
     * yarn队列名
     */
    private static final String YARN_QUEUE_NAME = "ds.dba";

    public static JavaSparkContext createSparkContext(Class<?> jars, boolean isLocalMode, String appName, int coreNum, Map<String, String> confParams) {

        String master;
        if (isLocalMode) {
            master = "local";
        } else {
            master = SparkYarnConf.MASTER_YARN_CLIENT;
        }

        System.setProperty("HADOOP_USER_NAME", "dota");
        SparkConf sparkConf = new SparkYarnConf(master).setAppName(appName).setClassAndJar(jars).setYarnQueue(YARN_QUEUE_NAME).
                setNumExectors(4).setExecutorCores(6)
                .setYarnJar("hdfs:///apps/spark/lib/spark-assembly-1.6.2-hadoop2.6.0.jar")
                .getSparkConf()
                .set(SPARK_AKKA_TIMEOUT, "600")
                .set("spark.executor.memory", "2048m")
                .set("spark.driver.memory","2048m")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.driver.extraClassPath", "/usr/hdp/current/hbase-client/lib/hbase-protocol-1.1.2.2.4.2.0-258-hadoop2.jar")
                .set("spark.executor.extraClassPath", "/usr/hdp/current/hbase-client/lib/hbase-protocol-1.1.2.2.4.2.0-258-hadoop2.jar");

        String sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.executor.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.executor.extraJavaOptions", sparkExecutorOpts);
        sparkExecutorOpts = "";
        try {
            sparkExecutorOpts = sparkConf.get("spark.driver.extraJavaOptions");
            if (sparkExecutorOpts == null) sparkExecutorOpts = "";
        } catch (NoSuchElementException ignore) {
        }
        sparkConf.set("spark.driver.extraJavaOptions", sparkExecutorOpts + " -XX:+UseConcMarkSweepGC");

        if (confParams != null) for (Map.Entry<String, String> e : confParams.entrySet())
            sparkConf.set(e.getKey(), e.getValue());

        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            sparkConf.setSparkHome(sparkHome);
        }
        return new JavaSparkContext(sparkConf);
    }


    public static JavaSparkContext createSparkContext(Class<?> jars, boolean isLocalMode, String appName, Integer coreNum) {
        return SparkUtil.createSparkContext(JavaSparkContext.jarOfClass(jars), isLocalMode, appName, coreNum);
    }


    public static JavaSparkContext createSparkContext(String[] jarpath, boolean isLocalMode, String appName, Integer coreNum) {
        MigSyncConfiguration conf = MigSyncConfiguration.getInstance();
        String masterUrl = conf.get(PARAM_SPARK_MASTER_URL);
        //  masterUrl = "local[3]";
        if (isLocalMode) {
            masterUrl = "local[*]";
        }
        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName(appName).setJars(jarpath).set(SPARK_AKKA_TIMEOUT, "600").set("spark.cores.max", String.valueOf(coreNum)).set("spark.default.parallelism", "2000").set("spark.executor.memory", "2g").set("spark.driver.memory", "10g").set("spark.speculation.multiplier", "1.5");

        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            sparkConf.setSparkHome(sparkHome);
        }
        return new JavaSparkContext(sparkConf);
    }

    /**
     * <li>功能描述：获取spark core的大小
     *
     * @return long
     * @author Administrator
     */
    public static int getSparkCore(Date beginDate, Date endDate) {
        long day = 0;
        try {
            day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000);
            return (int) ((day / 5 + 1) * 10 > 80 ? 80 : (day / 5 + 1) * 10);
            //System.out.println("相隔的天数="+day);
        } catch (Exception e) {
        }
        return 10;
    }

    /**
     * <li>功能描述：获取spark core的大小
     *
     * @return long
     * @author Administrator
     */
    public static int getSparkCore(long beginDate, long endDate) {
        long day = 0;

        try {
            day = (endDate - beginDate) / (24 * 60 * 60 * 1000);
            return (int) ((day / 5 + 1) * 10 > 80 ? 80 : (day / 5 + 1) * 10);
            //System.out.println("相隔的天数="+day);
        } catch (Exception e) {
        }
        return 10;
    }


}
