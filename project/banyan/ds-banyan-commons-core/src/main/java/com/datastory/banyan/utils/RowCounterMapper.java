package com.datastory.banyan.utils;

import com.datastory.banyan.spark.ScanFlushESMR;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * com.datastory.banyan.utils.RowCounterMapper
 *
 * @author lhfcws
 * @since 2017/6/26
 */
public class RowCounterMapper extends TableMapper<NullWritable, NullWritable> {

    public boolean filter(Result result) {
        return false;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        boolean isFiltered = filter(value);
        if (!isFiltered) {
            context.getCounter(ScanFlushESMR.ROW.ROWS).increment(1);
        }
    }
}
