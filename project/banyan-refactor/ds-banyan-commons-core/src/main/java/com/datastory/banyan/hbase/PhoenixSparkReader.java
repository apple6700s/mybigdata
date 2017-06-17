package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.io.DataSourceReader;
import com.datastory.banyan.io.DataSrcReadHook;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.spark.PhoenixRDD;
import org.apache.phoenix.spark.PhoenixRecordWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Some;
import scala.collection.Seq;

import java.sql.SQLException;
import java.util.*;

/**
 * com.datastory.banyan.hbase.PhoenixSparkReader
 *
 * @author lhfcws
 * @since 16/11/23
 */

public abstract class PhoenixSparkReader extends AbstractSparkRunner implements DataSourceReader {
    private List<DataSrcReadHook> hooks = new LinkedList<>();

    public abstract String getTable();

    @Override
    public List<DataSrcReadHook> getHooks() {
        return hooks;
    }

    @Override
    public Params read(Object input) {
        return null;
    }

    @Override
    public List<Params> reads(Iterator iterator) {
        return null;
    }

    /**
     * Usually for override
     *
     * @param predict
     * @param columns null for all columns
     * @throws SQLException
     */
    public void query(String predict, List<String> columns) throws SQLException {
        JavaRDD<PhoenixRecordWritable> rdd = queryRDD(predict, columns);
        rdd.foreachPartition(new VoidFunction<Iterator<PhoenixRecordWritable>>() {
            @Override
            public void call(Iterator<PhoenixRecordWritable> phoenixRecordWritableIterator) throws Exception {
                reads(phoenixRecordWritableIterator);
            }
        });
    }

    /**
     * @param predict
     * @param columns null for all columns
     * @throws SQLException
     */
    protected JavaRDD<PhoenixRecordWritable> queryRDD(String predict, List<String> columns) throws SQLException {
        Configuration conf = RhinoETLConfig.getInstance();
        JavaSparkContext sc = createSparkContext();
        if (columns == null) {
            PhoenixDriver phoenixDriver = PhoenixDriverFactory.getDriver();
            columns = phoenixDriver.getAllColumnNames(getTable());
        }
        Seq<String> columnSeq = BanyanTypeUtil.toSeq(columns);
        PhoenixRDD phoenixRDD = new PhoenixRDD(sc.sc(), getTable(), columnSeq,
                new Some<>(predict), new Some<>(PhoenixDriverFactory.getDriverZKConn()), conf);
        return phoenixRDD.toJavaRDD();
    }
}
