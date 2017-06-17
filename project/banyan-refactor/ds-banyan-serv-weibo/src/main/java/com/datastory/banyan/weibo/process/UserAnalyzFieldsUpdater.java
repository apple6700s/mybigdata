package com.datastory.banyan.weibo.process;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.ESSearcher;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.hbase.WbUserReader;
import com.datastory.banyan.weibo.util.RecentDaysStrategy;
import com.google.common.base.Function;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.quartz.SchedulerException;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.weibo.process.UserAnalyzFieldsUpdater
 * 定时更新用户分析字段
 * @author lhfcws
 * @since 16/11/30
 */

public class UserAnalyzFieldsUpdater extends AbstractSparkRunner {
    public static String INDEX_NAME = Tables.table(Tables.ES_WB_IDX);
    public static final String TYPE_USER = "user";

    private RecentDaysStrategy recentDaysStrategy = new RecentDaysStrategy(3);

    public UserAnalyzFieldsUpdater() {
        setCores(100);
    }

    public void setRecentDaysStrategy(RecentDaysStrategy recentDaysStrategy) {
        this.recentDaysStrategy = recentDaysStrategy;
    }

    /**
     * update a user by RecentDaysStrategy
     */
    public void update() {
        String updateDate = recentDaysStrategy.getEarliestUpdateDate();
        this.update(updateDate);
    }

    /**
     * update a user by give dateStr
     */
    public void update(String updateDate) {
        List<String> uids = readUidsByUpdateDate(updateDate);
        System.out.println("Update user size : " + uids.size());
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, "UserAnalyzFieldsUpdater", getCores() + "");
        try {
            jsc.parallelize(uids, getCores()).foreachPartition(
                    new VoidFunction<Iterator<String>>() {
                        @Override
                        public void call(Iterator<String> iter) throws Exception {
                            int size = 0;
                            RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_WBUSER_TBL));
                            while (iter.hasNext()) {
                                size++;
                                String uid = iter.next();
                                Params user = new WbUserReader().readUserNContent(uid);
                                if (user != null && !user.isEmpty()) {
                                    Params extUser = user;
//                                    Params extUser = WbUserAnalyzer.getInstance().advAnalyz(user);
                                    extUser.put("update_date", DateUtils.getCurrentTimeStr());
                                    // upsert into hbase and produce to es kafka
                                    putter.batchWrite(extUser);
                                }
                            }
                            putter.flush();
                            System.out.println("partition flush size : " + size);
                        }
                    });
        } finally {
            jsc.close();
            jsc.stop();
        }
    }

    public List<String> readUidsByUpdateDate(String updateDate) {
        final List<String> ret = new LinkedList<>();
        ESSearcher searcher = ESSearcher.getInstance();
        TransportClient client = searcher.client();
        QueryBuilder qb = new RangeQueryBuilder("update_date").from(updateDate);
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch(INDEX_NAME)
                .setTypes(TYPE_USER)
                .setSearchType(SearchType.SCAN)
                .setQuery(qb)
                .setFetchSource(new String[]{"id"}, new String[]{});
        searchRequestBuilder.setScroll(new TimeValue(60000)).setSize(500);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        ESSearcher.getInstance().searchNScroll(response, new Function<SearchHit, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable SearchHit searchHitFields) {
                ret.add((String) searchHitFields.getSource().get("id"));
                return null;
            }
        });
        return ret;
    }


    public static class CLI implements CliRunner, QuartzExecutor {
        public static final String DFT_CRON = "0 0 0 1/3 * ? *";

        @Override
        public Options initOptions() {
            Options options = new Options();
            options.addOption("q", false, "quartz mode");
            return options;
        }

        @Override
        public boolean validateOptions(CommandLine commandLine) {
            return true;
        }

        @Override
        public void start(CommandLine commandLine) {
            if (commandLine.hasOption("q")) {
                try {
                    QuartzJobUtils.createQuartzJob(DFT_CRON, "UserAnalyzFieldsUpdater", this);
                } catch (SchedulerException e) {
                    e.printStackTrace();
                }
            } else
                execute();
        }

        @Override
        public void execute() {
            new UserAnalyzFieldsUpdater().update();
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        AdvCli.initRunner(args, "UserAnalyzFieldsUpdater", new CLI());

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
