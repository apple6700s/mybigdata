package com.datastory.commons3.es.lucene_writer;

import okhttp3.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * com.datastory.commons3.es.lucene_writer.ShardInfo
 * Not thread-safe, Not support index aliases.
 *
 * @author lhfcws
 * @since 2017/5/3
 */
public class ShardLocation implements Serializable {

    /**
     * With http/https protocol
     */
    String host;
    String index;
    String httpUser = null;
    String httpPasswd = null;

    HashMap<Integer, EsShard> shardMap = new HashMap<>();

    public ShardLocation(String host, String index) {
        this.host = host;
        this.index = index;
    }

    public ShardLocation(String host, String index, String httpUser, String httpPasswd) {
        this.host = host;
        this.index = index;
        this.httpUser = httpUser;
        this.httpPasswd = httpPasswd;
    }

    public String getHost() {
        return host;
    }

    public String getIndex() {
        return index;
    }

    public String getHttpUser() {
        return httpUser;
    }

    public String getHttpPasswd() {
        return httpPasswd;
    }

    public void refreshShards() throws Exception {
        String url = host + "/_cat/shards?h=index,ip,node,shard,prirep&index=" + index;
        OkHttpClient client = new OkHttpClient.Builder().authenticator(new Authenticator() {
            @Override
            public Request authenticate(Route route, Response response) throws IOException {
                String credential = Credentials.basic(httpUser, httpPasswd);
                return response.request().newBuilder().header("Authorization", credential).build();
            }
        })
                .retryOnConnectionFailure(true)
                .readTimeout(60, TimeUnit.SECONDS)
                .build();

        Request request = new Request.Builder().url(url).get().build();
        Call call = client.newCall(request);
        Response response = null;
        String content = null;
        try {
            response = call.execute();
            content = response.body().string();
        } finally {
            if (response != null) {
                response.body().close();
                response.close();
            }
        }
        String[] contentLines = content.split("\n");

        for (String line : contentLines) {
            String[] items = line.replaceAll("[ ]+", " ").split(" ");
            String pri = items[4];
            if (!pri.equals("p"))
                continue;

            String index = items[0];
            if (!index.equals(this.index))
                continue;

            String ip = items[1];
            String nodeName = items[2];
            String shardId = items[3];

            Integer shardIdNum = Integer.valueOf(shardId);

            EsShard shard = new EsShard(index, shardIdNum, nodeName, ip);
            shardMap.put(shardIdNum, shard);
        }
    }

    public int getShardNum() {
        return shardMap.size();
    }

    public String getRemoteShardOutputPath(String root, int shardId) {
        EsShard shard = getShard(shardId);
        return root + "/" + shard.index + "/" + shardId + "/index";
    }

    public EsShard getShard(int shardId) {
        return shardMap.get(shardId);
    }

    @Override
    public String toString() {
        return "ShardInfo{" +
                "host='" + host + '\'' +
                ", index='" + index + '\'' +
                ", httpUser='" + httpUser + '\'' +
                ", httpPasswd='" + httpPasswd + '\'' +
                ", shardMap=" + shardMap +
                '}';
    }
}
