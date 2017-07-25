package com.datastory.banyan.weibo.process;


import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.hbase.HbaseScanner;
import com.datastory.banyan.spark.AbstractSparkYarnRunner;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.AdvUserJsonProcessor;
import com.datastory.banyan.weibo.analyz.BirthYearExtractor;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.quartz.SchedulerException;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * com.datastory.banyan.weibo.process.AdvUserHBaseUpdater
 * 用户高级字段更新入库。现在走kafka
 * @author lhfcws
 * @since 16/11/30
 */
@Deprecated
public class AdvUserHBaseUpdater extends AbstractSparkYarnRunner implements CliRunner, QuartzExecutor {
    public static boolean IGNORE_FLUSH = false;
    private static final String CRON = "0 0 0/1 * * *";
    private static Logger LOG = Logger.getLogger(AdvUserHBaseUpdater.class);
    static final byte[] B_FAMILY = "b".getBytes();
    static final byte[] F_FAMILY = "f".getBytes();
    static final byte[] R_FAMILY = "r".getBytes();
    static final byte[] IS_FLUSH_QUALIFIER = "is_flush".getBytes();

    static final HashMap<String, String> mappingField = BanyanTypeUtil.strArr2strMap(new String[]{
            "career", "company",
            "birthday", "birthdate",
            "education", "school",
            "tags", "tags",
    });


    private int cores = 50;

    public AdvUserHBaseUpdater() {
    }


    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.cores.max", cores + "");
        return sparkConf;
    }

    public void scan(String startDate, String endDate, boolean ignoreFlushed) throws IOException {
        Scan scan = HBaseUtils.buildScan(startDate, endDate);

        if (!ignoreFlushed) {
            FilterList filterList = new FilterList();
            SingleColumnValueFilter filter = new SingleColumnValueFilter(B_FAMILY, IS_FLUSH_QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, "0".getBytes());
            filterList.addFilter(filter);
            filter = new SingleColumnValueFilter(B_FAMILY, IS_FLUSH_QUALIFIER, CompareFilter.CompareOp.EQUAL, new NullComparator());
            filterList.addFilter(filter);
            scan.setFilter(filterList);
        }

        boolean onYarn = false;
        HbaseScanner scanner = new HbaseScanner(Tables.table(Tables.PH_WBADVUSER_TBL), scan);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = scanner.sparkScan(this.getClass(), cores, onYarn);
        JavaSparkContext jsc = scanner.getSparkHBaseScanner().getJsc();

        final Accumulator<Integer> readAcc = jsc.accumulator(0);
        final Accumulator<Integer> writeAcc = jsc.accumulator(0);
        final Accumulator<Integer> bflushAcc = jsc.accumulator(0);

        try {
            hbaseRDD
                    .repartition(cores)
                    .foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                @Override
                public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws Exception {
                    int size = 0;
                    LinkedList<Put> userPuts = new LinkedList<>();
                    LinkedList<Put> bPuts = new LinkedList<>();
                    LinkedList<Delete> dels = new LinkedList<Delete>();

                    while (iterator.hasNext()) {
                        try {
                            Result result = iterator.next()._2();
                            if (result == null || result.isEmpty())
                                continue;
                            size++;
                            byte[] rowKey = result.getRow();
                            Params p = new ResultRDocMapper(result).map();
                            String uid = p.getString("uid");
                            String isFlush = HBaseUtils.getValue(result, B_FAMILY, IS_FLUSH_QUALIFIER);
                            isFlush = BanyanTypeUtil.incString(isFlush, 1);
                            if (isFlush == null)
                                isFlush = "1";

                            // bPuts
                            Put bPut = new Put(rowKey);
                            bPut.addColumn(B_FAMILY, IS_FLUSH_QUALIFIER, isFlush.getBytes());
                            bPuts.add(bPut);
                            bflushAcc.add(1);
                            if (bPuts.size() > 3000) {
                                flushPuts(Tables.table(Tables.PH_WBADVUSER_TBL), bPuts);
                            }

                            // advUserField
                            byte[] userRowKey = BanyanTypeUtil.wbuserPK(uid).getBytes();
                            Put put = new Put(userRowKey);
                            String json = p.getString("json");
                            String type = p.getString("type");
                            Object parseRes = AdvUserJsonProcessor.process(type, json);
                            if ("bilateral_follow".equals(type)) {
                                Delete delete = new Delete(userRowKey);
                                delete.addFamily(F_FAMILY);
                                dels.add(delete);

                                Map<String, String> map = (Map<String, String>) parseRes;
                                if (map != null)
                                    for (Map.Entry<String, String> entry : map.entrySet()) {
                                        put.addColumn(F_FAMILY, entry.getKey().getBytes(), entry.getValue().getBytes());
                                    }
                            } else {
                                String value = (String) parseRes;
                                String field = mappingField.get(type);
                                if ("birthdate".equals(field)) {
                                    String birthYear = BirthYearExtractor.extract(value);
                                    if (birthYear == null)
                                        continue;
                                    put.addColumn(R_FAMILY, "birthyear".getBytes(), birthYear.getBytes());
                                }
                                put.addColumn(R_FAMILY, field.getBytes(), value.getBytes());
                            }
                            userPuts.add(put);
                            writeAcc.add(1);
                            if (userPuts.size() > 1000) {
                                flushDels(Tables.table(Tables.PH_WBUSER_TBL), dels);
                                flushPuts(Tables.table(Tables.PH_WBUSER_TBL), userPuts);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    readAcc.add(size);
                    System.out.println("partition size: " + size);

                    flushDels(Tables.table(Tables.PH_WBUSER_TBL), dels);
                    flushPuts(Tables.table(Tables.PH_WBUSER_TBL), userPuts);
                    flushPuts(Tables.table(Tables.PH_WBADVUSER_TBL), bPuts);
                }
            });

            System.out.println("[ADV] ====================================");
            System.out.println("[ADV] readAcc = " + readAcc.value());
            System.out.println("[ADV] writeAcc = " + writeAcc.value());
            System.out.println("[ADV] bflushAcc = " + bflushAcc.value());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jsc.close();
            jsc.stop();
        }
    }

    protected void flushDels(String table, LinkedList<Delete> dels) {
        if (dels.isEmpty()) return;
        HTableInterface hTable = null;
        try {
            hTable = HTableInterfacePool.get(table);
            hTable.delete(dels);
            dels.clear();
        } catch (Exception e) {

        } finally {
            try {
                HTableInterfacePool.close(hTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void flushPuts(String table, List<Put> puts) {
        if (puts.isEmpty()) return;
        HTableInterface hTable = null;
        try {
            hTable = HTableInterfacePool.get(table);
            hTable.put(puts);
            puts.clear();
        } catch (Exception e) {

        } finally {
            try {
                HTableInterfacePool.close(hTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void scan(Pair<String, String> pair, boolean ignoreFlushed) throws IOException {
        scan(pair.first, pair.second, ignoreFlushed);
    }

    public void scan(Date day, boolean ignoreFlushed) throws IOException {
        scan(DateUtils.genDateRange(day), ignoreFlushed);
    }

    public void run(Date d) {
        Pair<String, String> pair = DateUtils.genLastHourRange(d);
        System.out.println("[RANGE] " + pair);
        try {
            this.scan(pair, IGNORE_FLUSH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(String startDate, String endDate, boolean ignoreFlushed) {
        System.out.println("[ADV][RANGE] ignore flushed: " + ignoreFlushed + ", " + startDate + " -> " + endDate);
        try {
            this.scan(startDate, endDate, ignoreFlushed);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute() {
        run(new Date());
    }

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption("q", false, "quartz");
        options.addOption("hour", true, "dateStr like 20161130010000 , will run the data of last hour");
        options.addOption("date", true, "dateStr like 20161130010000 , will run the data of last day");
        options.addOption("startRow", true, "dateStr like 20161130010000 ");
        options.addOption("endRow", true, "dateStr like 20161130010000 ");
        options.addOption("h", false, "help");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine commandLine) {
        return true;
    }

    @Override
    public void start(CommandLine commandLine) {
        try {
            if (commandLine.hasOption("q")) {
                try {
                    QuartzJobUtils.createQuartzJob(CRON, "AdvUserUpdater", this);
                } catch (SchedulerException e) {
                    e.printStackTrace();
                }
            } else {
                if (commandLine.hasOption("hour")) {
                    String hour = commandLine.getOptionValue("hour");
                    Date date = DateUtils.parse(hour, DateUtils.DFT_TIMEFORMAT);
                    run(date);
                } else if (commandLine.hasOption("date")) {
                    String dateStr = commandLine.getOptionValue("date");
                    Date date = DateUtils.parse(dateStr, DateUtils.DFT_TIMEFORMAT);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    calendar.add(Calendar.DATE, -1);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.HOUR_OF_DAY, 1);

                    for (int i = 0; i < 24; i++) {
                        run(calendar.getTime());
                        calendar.add(Calendar.HOUR_OF_DAY, 1);
                    }
                } else {
                    String startRow = commandLine.getOptionValue("startRow");
                    String endRow = commandLine.getOptionValue("endRow");
                    run(startRow, endRow, IGNORE_FLUSH);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("[ADV] System started. " + new Date());

        if (args.length > 1)
            IGNORE_FLUSH = Boolean.valueOf(args[0]);

        AdvCli.initRunner(args, "AdvUserUpdaterCli", new AdvUserHBaseUpdater());

        long mainEndTime = System.currentTimeMillis();
        System.out.println("[ADV] Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
