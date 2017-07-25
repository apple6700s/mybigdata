package com.datastory.banyan.migrate1.tools;

/**
 * com.datastory.banyan.migrate1.tools.LuceneEsWriter
 *
 * @author lhfcws
 * @since 2017/5/3
 */
public class LuceneEsWriter {

//    public static void main(String[] args) throws Exception {
//        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
//        SparkRunner sparkRunner = new SparkRunner(Tables.table(Tables.PH_WBCNT_TBL), Tables.table(Tables.ES_WB_IDX), "weibo");
//        sparkRunner.run();
//        System.out.println("[PROGRAM] Program exited.");
//    }

//    public static class SparkRunner extends HbaseScanner.SparkHBaseScanner {
//        String table;
//        String index;
//        String type;
//        ShardInfo shardInfo;
//
//
//        public SparkRunner(String table, String index, String type) throws Exception {
//            super("LuceneEsWriter: " + table + " -> " + index, table, null);
//            this.table = table;
//            this.index = index;
//            this.type = type;
//            shardInfo = new ShardInfo("http://es-rhino.datatub.com", index);
//            shardInfo.refreshShards();
//        }
//
//        public void run() {
//            try {
//                final int shardNum = shardInfo.getShardNum();
//
//                JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = this.sparkScan(this.getClass(), shardNum);
//                hbaseRDD
//                        .flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Params>() {
//                            @Override
//                            public Iterable<Tuple2<Integer, Params>> call(Tuple2<ImmutableBytesWritable, Result> tpl) throws Exception {
//                                Result result = tpl._2();
//                                Params hbDoc = new ResultRDocMapper(result).map();
//                                Params esDoc = new WbCntHb2ESDocMapper(hbDoc).map();
//                                return Collections.singletonList(new Tuple2<>(new EsRouting().routing(esDoc, shardNum), esDoc));
//                            }
//                        })
//                        .groupByKey()
//                        .foreach(new VoidFunction<Tuple2<Integer, Iterable<Params>>>() {
//                            @Override
//                            public void call(Tuple2<Integer, Iterable<Params>> tpl) throws Exception {
//                                int shardId = tpl._1();
//                                Iterable<Params> iter = tpl._2();
//                                Configuration conf = new Configuration(RhinoETLConfig.getInstance());
//                                conf.set("dfs.replication", "1");
//
//                                final HashMap<String, String> configs = new HashMap<>();
//                                configs.put("lucene.hdfs.enable", "true");
//                                configs.put("lucene.hdfs.shard." + shardId + ".path", "");
//
//                                DsIndexWriter writer = new DsIndexWriter(index, shardId, configs, conf);
//                                for (Params p : iter) {
//                                    IndexRequest indexRequest = RequestFactory.indexRequest(index, type, p);
//                                    writer.write(indexRequest);
//                                }
//
//                                writer.flush();
//                                writer.close();
//                            }
//                        });
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                JavaSparkContext jsc = getJsc();
//                if (jsc != null)
//                    jsc.stop();
//            }
//        }
//    }
}
