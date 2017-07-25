package com.datastory.commons3.es.lucene_writer;

import com.datastory.commons3.es.copyData.EsIndexOperation;
import com.datastory.commons3.es.utils.GetShardMeta;

import java.io.IOException;

import static com.datastory.commons3.es.copyData.CopyHdfsData.getHostname;

/**
 * com.datastory.commons3.es.copyData.InitTranslog
 *
 * @author zhaozhen
 * @since 2017/6/14
 */

public class InitTranslog extends HdfsIndexWriterFactory {

    /**
     * 对应每个shard生成translog
     */
    public static void createTranslog(String esHost, int port, String cluster_name, String index) throws IOException {
        ShardLocation shardInfo = new ShardLocation(esHost, index);
        try {
            shardInfo.refreshShards();
        } catch (Exception e) {
            e.printStackTrace();
        }
        TranslogWriter writer = new TranslogWriter();
        int totalShards = EsIndexOperation.totalShards(esHost, port, cluster_name, index);
        GetShardMeta getShardMeta = new GetShardMeta();
        for (int i = 0; i < totalShards; i++) {
            String shardHost = getShardMeta.getShardHost(i, index);
            if (shardHost.equals(getHostname())) {
                String root = getShardMeta.getSubDir(i, index);
                EsShard shard = shardInfo.getShard(i);
                System.getProperties().put("path.home", "/opt/package/elasticsearch-2.3.3");
                String pathIndexData1 = shardInfo.getRemoteShardOutputPath(root, i);
                writer.tryInitTranslog(pathIndexData1, TranslogIniter.genUuid(shard.index, shard.host, shard.shardId));
            }
        }
    }

}


