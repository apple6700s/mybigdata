package com.datastory.banyan.retry;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.DateUtils;
import com.google.gson.JsonParseException;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.FileSystemHelper;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

/**
 * com.datastory.banyan.retry.RetryRunner
 *
 * @author lhfcws
 * @since 16/12/7
 */

public class RetryRunner implements Serializable, QuartzExecutor, CliRunner {
    static Configuration conf = new Configuration(RhinoETLConfig.getInstance());

    public RetryRunner() {
    }

    @Override
    public void execute() {
        Calendar calendar = Calendar.getInstance();
        System.out.println("Invoked RetryRunner at " + calendar.getTime());

        calendar.add(Calendar.DATE, -1);
        String dateStr = DateUtils.getDateStr(calendar.getTime());
        String retryLogPath = conf.get("retry.log.path", "/tmp");
//        String dir = retryLogPath + "/" + dateStr;

        FileSystemHelper fsh = FileSystemHelper.getInstance(conf);
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fileStatuses = fs.listStatus(new Path(retryLogPath));

            for (FileStatus fileStatus : fileStatuses) {
                String fn = fileStatus.getPath().getName();
                if (DateUtils.validateDate(fn) && fn.compareTo(dateStr) <= 0) {
                    FileStatus[] dbDirs = fs.listStatus(fileStatus.getPath());
                    for (FileStatus dbDir : dbDirs) {
                        String dir = dbDir.getPath().toString();
                        try {
                            Job job = runJob(dir);
                            if (job != null && job.isSuccessful()) {
                                System.out.println("[DELETE DIR] " + dir);
                                fsh.deleteFile(dir);
                            }
                        } catch (Exception e) {
                            System.err.println(e.getMessage() + " -- " + dir);
                        }
                    }
                    try {
                        if (fs.listStatus(fileStatus.getPath()).length == 0) {
                            fs.delete(fileStatus.getPath(), true);
                        }
                    } catch (Exception ignore) {
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Job runJob(String dir) throws Exception {
        System.out.println("[DIR] " + dir);
        Job job = createMRJob(dir);
        if (job != null) {
            System.out.println("[MR JOB] start running job.");
            job.waitForCompletion(true);
            System.out.println("[MR JOB] finish running job.");
        }
        return job;
    }

    protected Job createMRJob(String inputDir) throws IOException {
        if (!filterPath(inputDir)) {
            System.out.println("Skip path:  " + inputDir);
            return null;
        }
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.job.running.map.limit", "20");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName() + "-" + inputDir);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(inputDir));

        job.setMapperClass(getMapperClass());

        return job;
    }

    public Class<? extends Mapper> getMapperClass() {
        return RetryWriterMapper.class;
    }

    protected boolean filterPath(String path) {
        return true;
    }

    /**
     * Mapper
     */
    public static class RetryWriterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        static {
            RhinoETLConsts.MAX_IMPORT_RETRY = 0;
        }
        protected String fileName;
        protected int mode = -1;   // 0 for hbase, 1 for es
        protected String table;   // hbase table or es index
        protected String type;    // for es type
        protected transient RetryWriter retryWriter;
        protected Exception e = null;

        protected RetryWriter createHbaseRetryWriter() {
            HBaseRetryWriter hrw = new HBaseRetryWriter(table);
            return hrw;
        }

        protected ESWriter createEsWriter() {
            return null;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            RhinoETLConsts.MAX_IMPORT_RETRY = 0;

            InputSplit inputSplit = context.getInputSplit();
            fileName = ((FileSplit) inputSplit).getPath().getParent().getName();

            String[] names = fileName.split("\\.");
            if (names[0].equals("hbase"))
                mode = 0;
            else if (names[0].equals("es"))
                mode = 1;

            table = names[1];
            if (mode == 1) {
                type = names[2];
                ESRetryWriter esRetryWriter = new ESRetryWriter(table, type);
                esRetryWriter.init(createEsWriter());
                retryWriter = esRetryWriter;
            } else if (mode == 0) {
                retryWriter = createHbaseRetryWriter();
            }
            System.out.println("RetryWriter: " + retryWriter);
            System.out.println("parent name: " + fileName);
            System.out.println("Setup Retry: " + ((FileSplit) inputSplit).getPath());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(RETRY_COUNTER.READ).increment(1);
            String record = Bytes.toString(value.getBytes());
            if (record.isEmpty()) return;

            System.out.println("[DEBUG] " + record);

            if (retryWriter != null) {
                try {
                    retryWriter.write(record);
                    context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
                } catch (Exception e1) {
                    if (!(e1 instanceof JsonParseException) && !(e1 instanceof NumberFormatException))
                        e = e1;
                    System.err.println(e1.getMessage() + "  --  " + record);
                    context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (retryWriter != null) {
                try {
                    retryWriter.flush();
                } catch (Exception e1) {
                    e = e1;
                }
            }
            if (e != null) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Counter
     */
    public enum RETRY_COUNTER {
        READ, WRITE
    }

    protected static final String DFT_CRON = "0 1 0 * * ?";

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption("q", false, "quartz mode ");
        options.addOption("cron", true, " cron");
        options.addOption("test", true, "give a input path to test");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine commandLine) {
        return true;
    }

    @Override
    public void start(CommandLine cmd) {
        String cron = cmd.getOptionValue("cron");
        if (StringUtil.isNullOrEmpty(cron))
            cron = DFT_CRON;

        if (cmd.hasOption("q")) {
            try {
                QuartzJobUtils.createQuartzJob(cron, "RetryRunner", this);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        } else if (!cmd.hasOption("test"))
            execute();
        else {
            try {
                String path = cmd.getOptionValue("test");
                runJob(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * MAIN
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        RetryRunner retryRunner = new RetryRunner();
        System.out.println("Start " + retryRunner.getClass().getSimpleName());
        AdvCli.initRunner(args, retryRunner.getClass().getSimpleName(), retryRunner);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
