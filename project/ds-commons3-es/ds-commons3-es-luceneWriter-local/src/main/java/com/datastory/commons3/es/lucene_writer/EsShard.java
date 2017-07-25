package com.datastory.commons3.es.lucene_writer;

import java.io.Serializable;

/**
 * com.datastory.commons3.es.lucene_writer.Shard
 *
 * @author lhfcws
 * @since 2017/6/12
 */
public class EsShard implements Serializable {
    String index;
    int shardId;
    String nodeName;
    String host;
    int nodeId = 0;

    public EsShard(String index, int shardId, String nodeName, String host) {
        this.index = index;
        this.shardId = shardId;
        this.nodeName = nodeName;
        this.host = host;
    }

    public EsShard(String index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "EsShard{" +
                "index='" + index + '\'' +
                ", shardId=" + shardId +
                ", nodeName='" + nodeName + '\'' +
                ", host='" + host + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EsShard esShard = (EsShard) o;

        if (shardId != esShard.shardId) return false;
        if (!index.equals(esShard.index)) return false;
        if (!nodeName.equals(esShard.nodeName)) return false;
        return host.equals(esShard.host);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shardId;
        result = 31 * result + nodeName.hashCode();
        result = 31 * result + host.hashCode();
        return result;
    }
}
