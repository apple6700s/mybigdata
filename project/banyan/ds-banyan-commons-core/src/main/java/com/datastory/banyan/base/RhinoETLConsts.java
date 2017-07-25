package com.datastory.banyan.base;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author sezina
 * @since 7/14/16
 */
public class RhinoETLConsts {

    public static final String RHINO_ETL_CONF = "banyan-etl-config.xml";

    // spark
    public static final String SPARK_MASTER_URL = "spark.master.url";

    //验证引擎所调用的execute abel.chan
    public static final String VALIDATE_EXECUTE_LIST = "validate.execute.list";
    //验证引擎结果的统计计算对象
    public static final String VALIDATE_STATS_INSTANCE = "validate.stats.instance";

    //zookeeper
    public static final String ZOOKEEPER_LIST = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";
    public static final String KAFKA_ZOOKEEPER_LIST = "kafka.hbase.zookeeper.quorum";
    public static final String KAFKA_ZOOKEEPER_PORT = "kafka.hbase.zookeeper.property.clientPort";

    //kafka broker list
    public static final String KAFKA_BROKER_LIST = "kafka.broker.list";

    // es consts
    public static final String ES_HOSTS = "es.hosts";
    public static final String ES_HOSTS_BULK = "es.hosts.bulk";
    public static final String ES_HOSTS_QUERY = "es.hosts.query";
    public static final String ES_CLUSTER_NAME = "es.cluster.name";

    public static final String SEPARATOR = "|";

    public static final byte[] R = "r".getBytes();
    public static final byte[] F = "f".getBytes();
    public static final byte[] B = "b".getBytes();
    public static final byte[] M = "m".getBytes();
    public static final byte[] A = "a".getBytes();
    public static final byte[] UID = "uid".getBytes();
    public static final byte[] MID = "mid".getBytes();
    public static final byte[] CONTENT = "content".getBytes();
    public static final byte[] PUBLISH_DATE = "publish_date".getBytes();
    public static final byte[] UPDATE_DATE = "update_date".getBytes();

    public static final String UNK_CITY_TYPE = "-2";

    public static final String DFT_TIMEFORMAT = "yyyyMMddHHmmss";
    public static final String DFT_DAYFORMAT = "yyyyMMdd";

    public static final String SRC_WB = "wb";
    public static final String SRC_NF = "nf";
    public static final String SRC_WX = "wx";
    public static final String SRC_ECOM = "ecom";
    public static final String SRC_VIDEO = "vd";

    public static final String FOLLOW_LIST_KEY =  "follower";
//    public static final String FOLLOW_LIST_KEY =  "follow_list";

    public static final int MAX_ANALYZ_LEN = 100;
    public static int MAX_IMPORT_RETRY = 3;
    // 当用到HBase versions特性时，默认的值；没用到versions特性的HBase默认只存最新的版本。
    public static final int DFT_VERSIONS_NUM = 30;

    public static final String[] ES_COMMON_SHORT_FIELDS = {
            "sentiment", "is_robot", "is_ad", "gender", "msg_type",
            "is_parent", "is_main_post"
    };

    public static final String [] ES_COMMON_LIST_FIELDS = {
            "topics", "emoji", "keywords", "source",
    };

    public static final String [] ES_COMMON_FLOAT_FIELDS = {
            "price", "promo_price", "platform_score", "score",
    };

    public static final String [] HB_M_FIELDS = {
            "taskId",
    };
    public static final Set<String> HB_M_FIELDS_SET = new HashSet<>(Arrays.asList(HB_M_FIELDS));

    public static final int HB_M_VERSIONS_NUM = 30;
}
