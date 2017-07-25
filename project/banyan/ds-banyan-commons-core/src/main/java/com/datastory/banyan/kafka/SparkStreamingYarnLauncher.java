package com.datastory.banyan.kafka;

import com.datastory.commons.spark.client.SparkYarnClusterSubmit;
import com.datastory.commons.spark.client.SparkYarnConf;
import com.datastory.commons.spark.client.streaming.StreamingYarnConf;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.spark.SparkConf;

import java.lang.management.ManagementFactory;

import static com.datastory.commons.spark.client.SparkYarnConf.MASTER_YARN_CLUSTER;

/**
 * com.datastory.banyan.kafka.SparkStreamingYarnLauncher
 * <p>
 * This is only a yarn cluster consumer launcher.
 *
 * @author lhfcws
 * @since 2017/1/3
 */
@Deprecated
public abstract class SparkStreamingYarnLauncher {
    public abstract Class getConsumerClass();

    public String getYarnQueue() {
        return "default";
    }

    public SparkYarnConf createSparkYarnConf() throws IllegalAccessException, InstantiationException {
        SparkYarnConf sparkYarnConf;
//            SparkYarnClusterKafkaConsumer consumer = getConsumerInstance();
        Class klass = getConsumerClass();
        SparkYarnClusterKafkaConsumer consumer = (SparkYarnClusterKafkaConsumer) klass.newInstance();
        String appName = klass.getSimpleName();
        // yarn-client
//        sparkYarnConf = new SparkYarnConf();
        // yarn-cluster
        StrParams conf = consumer.customizedSparkConfParams();
        int executorCores = conf.getOrElse("spark.executor.cores", 2);

        sparkYarnConf = new SparkYarnConf(MASTER_YARN_CLUSTER);
        sparkYarnConf.setYarnQueue(getYarnQueue())
                .setAppName(appName)
                .setDriverMemory(conf.getOrElse("spark.driver.memory", "2048m"))
                .setDriverCores(conf.getOrElse("spark.driver.cores", 1))
                .setNumExectors(conf.getOrElse("spark.executor.instances", consumer.getSparkCores() / executorCores + (consumer.getSparkCores() % executorCores > 0 ? 1 : 0)))
                .setExecutorCores(executorCores)
                .setExecutorMemory(conf.getOrElse("spark.executor.memory", "1500m"))
                .setClassAndJar(klass)
        ;
        SparkConf sc = sparkYarnConf.getSparkConf();
        sc
                .set("spark.metrics.conf.driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs:///apps/spark/history/")
                .set("spark.yarn.historyServer.address", "alps1:18080")
                .set("spark.history.ui.port", "18080")
                .set("spark.history.fs.logDirectory", "hdfs:///apps/spark/history")
                .set("spark.history.fs.cleaner.enabled", "true")
                .set("spark.yarn.scheduler.heartbeat.interval-ms", "5000")
        ;

        StreamingYarnConf streamingConf = new StreamingYarnConf(sparkYarnConf);
        streamingConf.setMaxRate(3000);
        streamingConf.setMinRate(1);
        sparkYarnConf = streamingConf.getSparkYarnConf();

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
        SparkStreamingYarnLauncher launcher = new SparkStreamingYarnLauncher() {
            @Override
            public Class getConsumerClass() {
                return klass;
            }
        };

        // create sparkYarnConf
        SparkYarnConf sparkYarnConf = launcher.createSparkYarnConf();
        // change some configuration here

        // submit
        launcher.submit(sparkYarnConf);

        System.out.println("[PROGRAM] Program exited.");
    }
}
