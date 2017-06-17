package com.datastory.banyan.weibo.spark;

import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.spark.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.spark.UidPartitioner
 *
 * @author lhfcws
 * @since 2016/12/26
 */
public class UidPartitioner extends Partitioner {
    private static final Map<Character, Integer> sp = new HashMap<>();
    static {
        sp.put('a', 10);
        sp.put('b', 11);
        sp.put('c', 12);
        sp.put('d', 13);
        sp.put('e', 14);
        sp.put('f', 15);
        sp.put('0', 0);
        sp.put('1', 1);
        sp.put('2', 2);
        sp.put('3', 3);
        sp.put('4', 4);
        sp.put('5', 5);
        sp.put('6', 6);
        sp.put('7', 7);
        sp.put('8', 8);
        sp.put('9', 9);
    }

    private int partNum;

    public UidPartitioner(int partNum) {
        this.partNum = partNum;
    }

    @Override
    public int numPartitions() {
        return partNum;
    }

    @Override
    public int getPartition(Object key) {
        String s = Md5Util.md5(key.toString()).substring(0, 3);
        try {
            int m = 256 * sp.get(s.charAt(0)) + 16 * sp.get(s.charAt(1)) + sp.get(s.charAt(0));
            return m % numPartitions();
        } catch (Exception e) {
            return s.hashCode() % numPartitions();
        }
    }
}
