package com.datastory.banyan.spark;

import com.datastory.commons.spark.client.SparkYarnClusterSubmit;
import com.datastory.commons.spark.client.SparkYarnConf;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static com.datastory.commons.spark.client.SparkYarnConf.MASTER_YARN_CLUSTER;

/**
 * com.datastory.banyan.spark.SparkYarnLauncher
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public abstract class SparkYarnLauncher implements Serializable {
    private static String name = "";

    public abstract Class getRunnerClass();

    public SparkYarnConf createSparkYarnConf() throws IllegalAccessException, InstantiationException {
        SparkYarnConf sparkYarnConf;
        Class klass = getRunnerClass();
        SparkYarnRunner runner = (SparkYarnRunner) klass.newInstance();
        String appName = runner.getAppName() + " " + name;
        // yarn-cluster
        StrParams conf = runner.customizedSparkConfParams();
        int cores = conf.getOrElse("spark.cores.max", 1);
        int executorCores = conf.getOrElse("spark.executor.cores", 2);

        sparkYarnConf = new SparkYarnConf(MASTER_YARN_CLUSTER);
        sparkYarnConf.setYarnQueue(runner.getYarnQueue())
                .setAppName(appName)
                .setDriverMemory(conf.getOrElse("spark.driver.memory", "2048m"))
                .setDriverCores(conf.getOrElse("spark.driver.cores", 1))
                .setNumExectors(conf.getOrElse("spark.executor.instances", cores / executorCores + (cores % executorCores > 0 ? 1 : 0)))
                .setExecutorCores(executorCores)
                .setExecutorMemory(conf.getOrElse("spark.executor.memory", "2048m"))
                .setClassAndJar(klass)
        ;
        SparkConf sc = sparkYarnConf.getSparkConf();
        sc
                .set("spark.default.parallelism", cores + "")
                .set("spark.metrics.conf.driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs:///apps/spark/history/")
                .set("spark.yarn.historyServer.address", "alps1:18080")
                .set("spark.history.ui.port", "18080")
                .set("spark.history.fs.logDirectory", "hdfs:///apps/spark/history")
                .set("spark.history.fs.cleaner.enabled", "true")
                .set("spark.yarn.scheduler.heartbeat.interval-ms", "5000")
        ;

        return sparkYarnConf;
    }

    public void submit(SparkYarnConf sparkYarnConf) {
        System.out.println("SparkYarnConf - " + sparkYarnConf);

        System.out.println("-------------------start------------------------");
        try {
            SparkYarnClusterSubmit.submit(sparkYarnConf);
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
        } finally {
            System.out.println("-------------------finish-----------------------");
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

        if (args.length == 0) {
            System.err.println("Not implemented launcher class or not given a consumer to launch.");
            return;
        }
        String cn = args[0];


        final Class klass = Class.forName(cn);
        SparkYarnLauncher launcher = new SparkYarnLauncher() {
            @Override
            public Class getRunnerClass() {
                return klass;
            }
        };

        // create sparkYarnConf
        String[] argses = null;
        if (args.length > 1) {
            argses = (String[]) ArrayUtils.subarray(args, 1, args.length);
            name = Arrays.toString(argses);
        }
        SparkYarnConf sparkYarnConf = launcher.createSparkYarnConf();
        if (args.length > 1) {
            sparkYarnConf.setClusterArgsWithBase64(argses);
        }
        // change some configuration here

        // submit
        launcher.submit(sparkYarnConf);

        System.out.println("[PROGRAM] Program exited.");
    }
}
