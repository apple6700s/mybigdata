package com.datastory.commons3.es.bulk_writer.utils;

import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DsEsUtils {

    public static ThreadFactory createDaemonThreadFactory(final String namePrefix) {
        return new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(namePrefix + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
    }

    public static boolean isEmptyString(String str) {
        return str == null || str.length() == 0;
    }

    public static InetSocketTransportAddress parseTransportAddress(String address) throws Exception {
        if (isEmptyString(address)) {
            throw new IllegalArgumentException("address is empty string");
        }
        String[] strs = address.split(":");
        return new InetSocketTransportAddress(InetAddress.getByName(strs[0]),
                Integer.parseInt(strs[1]));
    }

    public static HashFunction hashFunc = new Murmur3HashFunction();

    public static int generateShardId(String id, String routing, int numOfShards) {
        final int hash;
        if (routing == null) {
            hash = hashFunc.hash(id);
        } else {
            hash = hashFunc.hash(routing);
        }
        return MathUtils.mod(hash, numOfShards);
    }
}
