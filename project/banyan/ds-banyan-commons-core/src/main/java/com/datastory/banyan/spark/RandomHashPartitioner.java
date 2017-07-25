package com.datastory.banyan.spark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.util.Utils;

import java.util.concurrent.ThreadLocalRandom;

/**
 * com.datastory.banyan.spark.RandomHashPartitioner
 *
 * @author lhfcws
 * @since 2017/1/3
 */
public class RandomHashPartitioner extends HashPartitioner {
    private ThreadLocalRandom random;
    public RandomHashPartitioner(int partitions) {
        super(partitions);
        random = ThreadLocalRandom.current();
    }

    @Override
    public int getPartition(Object key) {
        int r = random.nextInt();
        if (key == null)
            return Utils.nonNegativeMod(r, numPartitions());
        else {
            return Utils.nonNegativeMod(r * key.hashCode(), numPartitions());
        }
    }
}
