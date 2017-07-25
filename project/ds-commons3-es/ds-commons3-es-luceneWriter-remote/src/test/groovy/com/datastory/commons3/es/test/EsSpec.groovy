package com.datastory.commons3.es.test

import com.datastory.commons3.es.lucene_writer.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.elasticsearch.action.index.IndexRequest
import spock.lang.Specification

/**
 * com.datastory.commons3.es.test.EsSpec
 * Groovy specification class
 *
 * @author lhfcws
 * @since 2017/4/28
 */
class EsSpec extends Specification {
    static final String CLUSTER = "lhfcws_cluster";
    static final String INDEX = "ds-es1";
    static final String TYPE = "t";

    def "get shard info"() {
        when:
        ShardLocation shardInfo = new ShardLocation("http://es-rhino.datatub.com", "dt-rhino-weibo-v2", "admin", "adminamkt");
        shardInfo.refreshShards()

        then:
        println shardInfo
        true
    }

    /**
     * 直接在es集群写入的示例
     */
    def "local write data"() {
        when:
        System.properties.put("path.home", "/opt/package/elasticsearch-2.3.3");
        String index = INDEX;
        int shardId = 0;
        int totalShards = 1;


        Map<String, String> configs = [
//                "index.analysis.analyzer.wordsEN.type": WordsENAnalyzerProvider.class.canonicalName,
//                "index.analysis.analyzer.wordsEN.tokenizer": WordsENTokenizerFactory.class.canonicalName,
"test.config": "0"
        ]

        LocalIndexWriter writer = new LocalIndexWriterFactory().createLocalIndexWriter(index, shardId, configs);
//        LocalIndexWriter writer = new LocalIndexWriterFactory().createLocalIndexWriter(index, shardId);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
            Map<String, String> params = FakeData.fake(i);
            String id = params.get("id");
            params.put("name", "测试iphone")
            int toShard = EsRouting.routing(id, totalShards);
            if (toShard == 0) {
                println "0 => " + params
                IndexRequest indexRequest = RequestFactory.indexRequest(INDEX, TYPE, params);
                writer.write(indexRequest);
            }
        }

        writer.flush();
        writer.close();

        println "Cost time (ms) : " + (System.currentTimeMillis() - start)

        then:
        true
    }

    def "hdfs write data"() {
        when:
        int DOCS = 5;
        int totalShards = 1;

        Configuration conf = new Configuration();
        conf.addResource("core-site.xml")
        conf.addResource("hdfs-site.xml")

        String index = INDEX;
        int shardId = 0;

        System.properties.put("path.home", "/tmp/"); // it can be any arbitary path
        ShardLocation shardInfo = new ShardLocation("http://datatub1:9200", "ds-es1");
        shardInfo.refreshShards()
        EsShard shard0 = shardInfo.getShard(0);

        String pathIndexData = shardInfo.getRemoteShardOutputPath("/tmp/lhfcws", 0);
        Map<String, String> configs = [
                "lucene.hdfs.enable"      : "true",
                "lucene.hdfs.shard.0.path": pathIndexData
        ];

        HdfsIndexWriterFactory factory = new HdfsIndexWriterFactory();
        HdfsIndexWriter writer = factory.createHdfsIndexWriter(index, shardId, configs, conf);
        factory.tryInitTranslog(writer, pathIndexData, TranslogIniter.genUuid(shard0.index, shard0.host, shard0.shardId));

        for (int i = 0; i < DOCS; i++) {
            try {
                Map<String, String> params = FakeData.fake(i);
                String id = params.get("id");
                int toShard = EsRouting.routing(id, totalShards);
                if (toShard == 0) {
                    params.put("name", i + "测试iphone")
                    println "0 => " + params
                    IndexRequest indexRequest = RequestFactory.indexRequest(INDEX, TYPE, params);
                    writer.write(indexRequest);
                }
                if (i % 1000 == 0)
                    println("Progress: " + i);
            } catch (Exception e) {
                System.err.println("ERROR Progress : " + i);
                e.printStackTrace();
            }
        }

        writer.flush();
        writer.close();

        then:
        true
    }

    def "should write hdfs by shard"() {
        when:
        int DOCS = 5000;
        String INDEX = "ds-es1";
        String HDFS_ROOT = "/tmp/lhfcws";

        Configuration conf = new Configuration();
        conf.addResource("core-site.xml")
        conf.addResource("hdfs-site.xml")

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(HDFS_ROOT), true);
        fs.mkdirs(new Path(HDFS_ROOT));

        ShardLocation shardInfo = new ShardLocation("http://datatub2:9200", INDEX);
        shardInfo.refreshShards();
        int totalShards = shardInfo.getShardNum();
        println shardInfo

        Map<Integer, HdfsIndexWriter> writers = new HashMap<>();
        Map<Integer, LinkedList<IndexRequest>> buffer = new HashMap<>();

        for (int shardId = 0; shardId < shardInfo.getShardNum(); shardId++) {
            EsShard shard = shardInfo.getShard(shardId);
            String pathIndexData = shardInfo.getRemoteShardOutputPath(HDFS_ROOT, shardId);
            Map<String, String> configs = [
                    "lucene.hdfs.enable"                      : "true",
                    ("lucene.hdfs.shard." + shardId + ".path"): pathIndexData
            ];

            HdfsIndexWriterFactory factory = new HdfsIndexWriterFactory();
            HdfsIndexWriter writer = factory.createHdfsIndexWriter(INDEX, shardId, configs, conf);
            factory.tryInitTranslog(writer, pathIndexData, TranslogIniter.genUuid(shard));

            writers.put(shardId, writer);
            buffer.put(shardId, new LinkedList<IndexRequest>());
        }

        long start = System.currentTimeMillis();
        int i = 0;
        try {
            for (; i < DOCS; i++) {
                try {
                    Map<String, String> params = FakeData.fake(i);
                    String id = params.get("id");
                    int toShard = EsRouting.routing(id, totalShards);
                    params.put("name", i + "测试iphone")
//                println toShard + " => " + params
                    IndexRequest indexRequest = RequestFactory.indexRequest(INDEX, TYPE, params);
                    buffer.get(toShard).add(indexRequest);

                    if (i % 1000 == 0) {
                        println("Progress: " + i);
                        for (int j = 0; j < shardInfo.getShardNum(); j++) {
                            LinkedList<IndexRequest> buf = new LinkedList<>(buffer.get(j));
                            buffer.get(j).clear();
                            writers.get(j).write(buf);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("ERROR Progress : " + i);
                    e.printStackTrace();
                    break;
                }
            }
        } finally {
            long diff = System.currentTimeMillis() - start;
            println "Cost time (ms) : " + diff
            println "Final Record: " + i
        }

        println "All finish writing.";
        for (int shardId : writers.keySet()) {
            HdfsIndexWriter writer = writers.get(shardId);
            writer.flush();
            writer.close();
            println "Close writer : " + shardId
        }

        then:
        true
    }

    def "should write local and flush to remote"() {
        when:
        int DOCS = 1000000;
        String INDEX = "ds-es1";
        String HDFS_ROOT = "/tmp/lhfcws";

        Configuration conf = new Configuration();
        conf.addResource("core-site.xml")
        conf.addResource("hdfs-site.xml")

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(HDFS_ROOT), true);
        fs.mkdirs(new Path(HDFS_ROOT));

        ShardLocation shardInfo = new ShardLocation("http://datatub2:9200", INDEX);
        shardInfo.refreshShards();
        int totalShards = shardInfo.getShardNum();
        println shardInfo

        Map<Integer, LocalBufferHdfsWriter> writers = new HashMap<>();
        Map<Integer, LinkedList<IndexRequest>> buffer = new HashMap<>();
        HdfsIndexWriterFactory factory = new HdfsIndexWriterFactory();

        for (int shardId = 0; shardId < shardInfo.getShardNum(); shardId++) {
            EsShard shard = shardInfo.getShard(shardId);
            String translogUuid = TranslogIniter.genUuid(shard);
            String remotePath = shardInfo.getRemoteShardOutputPath(HDFS_ROOT, shardId);
            Map<String, String> configs = [
                    "lucene.hdfs.enable"                      : "true",
                    ("lucene.hdfs.shard." + shardId + ".path"): remotePath,
                    "path.data"                               : "/tmp/esdata"
            ];

            LocalBufferHdfsWriter writer = new LocalBufferHdfsWriter.Builder(conf)
                    .setIndexName(INDEX)
                    .setShardId(shardId)
                    .build(configs)
            ;
            println translogUuid
            factory.tryInitTranslog(writer.getBufferWriter(), translogUuid)
            factory.tryInitTranslog(writer.getMergeWriter(), translogUuid)

            writers.put(shardId, writer);
            buffer.put(shardId, new LinkedList<IndexRequest>());
        }

        long start = System.currentTimeMillis();
        int i = 0;
        try {
            for (; i < DOCS; i++) {
                try {
                    Map<String, String> params = FakeData.fake(i);
                    String id = params.get("id");
                    int toShard = EsRouting.routing(id, totalShards);
                    params.put("name", i + "测试iphone")
//                println toShard + " => " + params
                    IndexRequest indexRequest = RequestFactory.indexRequest(INDEX, TYPE, params);
                    buffer.get(toShard).add(indexRequest);

                    if (i % 1000 == 0) {
                        println("Progress: " + i);
                        for (int j = 0; j < shardInfo.getShardNum(); j++) {
                            LinkedList<IndexRequest> buf = new LinkedList<>(buffer.get(j));
                            buffer.get(j).clear();
                            writers.get(j).write(buf);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("ERROR Progress : " + i);
                    e.printStackTrace();
                    break;
                }
            }

            println "All finish writing.";
            for (int shardId : writers.keySet()) {
                LocalBufferHdfsWriter writer = writers.get(shardId);
                writer.flush();
                writer.close();
                println "Close writer : " + shardId
            }
        } finally {
            long diff = System.currentTimeMillis() - start;
            println "Final Record: " + i
            println "Cost time (ms) : " + diff
        }

        then:
        true
    }
}
