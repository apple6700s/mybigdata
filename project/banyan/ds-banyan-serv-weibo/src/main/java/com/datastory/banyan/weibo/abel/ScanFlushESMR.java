package com.datastory.banyan.weibo.abel;

import com.datastory.banyan.base.RhinoETLConfig;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

/**
 * com.datastory.banyan.spark.ScanFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class ScanFlushESMR implements Serializable {

    public int getReducerNum() {
        return 40;
    }

    public Scan buildAllScan(Filter filter) {
        Scan scan = HBaseUtils.buildScan();
        if (filter != null)
            scan.setFilter(filter);
        return scan;
    }

    public Scan buildAllScan() {
        return buildAllScan(null);
    }

    public Job buildJob(String table, Scan scan, Class<? extends TableMapper> mapperClass, Class<? extends Reducer> reducerClass
            , String outputPath) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

        job.setInputFormatClass(TableInputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);

        Class<?> mapOutputKeyClass = Text.class;
        Class<?> mapOutputValueClass = Params.class;

        if (reducerClass == null) {
            mapOutputKeyClass = NullWritable.class;
            mapOutputValueClass = NullWritable.class;
            job.setNumReduceTasks(0);
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(reducerClass);
            job.setMapOutputKeyClass(mapOutputKeyClass);
            job.setMapOutputValueClass(mapOutputValueClass);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(getReducerNum());
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setJarByClass(this.getClass());

        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, mapOutputKeyClass, mapOutputValueClass, job);


//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    /**
     * Mapper
     */
    public static abstract class ScanMapper extends TableMapper<Text, Params> {
        protected static Murmur3HashFunction hashFunc = new Murmur3HashFunction();
        static Random random = new Random();

        protected String routing(String pk) {
//            String id = pk.substring(3);
//            int i = hashFunc.hash(id);
//            return i + "";
//            return random.nextInt(1000) + "";
            return pk;
        }

//        @Override
//        protected abstract void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//            context.getCounter(ROW.READ).increment(1);
//
//            if (value == null || value.isEmpty()) {
//                context.getCounter(ROW.FILTER).increment(1);
//                return;
//            }
//            String pk = new String(value.getRow());
//            if (pk.length() < 4) {
//                context.getCounter(ROW.FILTER).increment(1);
//                return;
//            }
//
////            Params hbDoc = new ResultRDocMapper(value).map();
////            Params esDoc = mapDoc(hbDoc);
//
////            if (esDoc != null) {
////                String prefix = pk.substring(0, 3);
////                context.write(new Text(prefix), esDoc);
////                context.getCounter(ROW.SHUFFLE).increment(1);
////            } else {
////                context.getCounter(ROW.ERROR).increment(1);
////                System.err.println("[ErrHbDoc] " + hbDoc);
////            }
//        }
//    }
    }

    /**
     * Reducer
     */
    public static abstract class FlushESReducer extends Reducer<Text, Params, Text, Text> {
        protected static Logger LOG = Logger.getLogger(FlushESReducer.class);
        protected long startTime;

        static {
            RhinoETLConfig.getInstance().set("banyan.es.hooks.enable", "false");
        }

        public boolean isDebug() {
            return false;
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            startTime = System.currentTimeMillis();
            System.out.println("[TIME] start " + startTime);
        }

        @Override
        protected void reduce(Text key, Iterable<Params> values, final Context context) throws IOException, InterruptedException {

            long size = 0;
            for (Params p : values) {
                size++;
                try {
                    context.write(key, new Text(FastJsonSerializer.serialize(p)));
                    context.getCounter(ROW.WRITE).increment(1);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!isDebug()) {
                //ESWriterAPI writer = getESWriter();
                //writer.flush();
//                writer.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("[Time] end " + endTime + " , cost " + (endTime - startTime));
        }
    }

    public enum ROW {
        READ, WRITE, FILTER, PASS, ERROR, OTHER,
        MAP, LIST, SHUFFLE, ROWS
    }
}
