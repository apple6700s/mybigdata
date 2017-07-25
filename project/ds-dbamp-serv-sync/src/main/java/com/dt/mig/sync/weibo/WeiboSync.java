package com.dt.mig.sync.weibo;

import com.dt.mig.sync.BaseReader;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.flush.FlushUserEsSpark;
import com.dt.mig.sync.utils.SparkUtil;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by abel.chan on 16/11/18.
 */
//@Deprecated
public class WeiboSync extends BaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboSync.class);

    public WeiboSync() {
        super();
    }


    public static final String[] SEARCH_ES_FILEDS = new String[]{MigSyncConsts.ES_WEIBO_WEIBO_CONTENT};

    @Override
    public void execute(long start, long end) throws Exception {

        initQueryBuilder(start, end);//初始化builder

        WeiboSyncSpark syncSpark = new WeiboSyncSpark();
        syncSpark.run(queryBuilder, start, end, SparkUtil.getSparkCore(start, end));

        //更新用户
        LOG.info("执行补用户的meta_group,topics数据!");
        //开始补user的metagroup数据。
        FlushUserEsSpark flushUserEsSpark = new FlushUserEsSpark();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        flushUserEsSpark.setRangeDate(sdf.format(new Date(start)), sdf.format(new Date(end)));
        flushUserEsSpark.run();
    }

    public QueryBuilder buildQueryBuilder(long start, long end) {
        //由于数据量太大,所以一次只使用一个查询字段。
        return CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getWeiboDocId(), MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);
    }

    @Override
    protected void executeParent() throws Exception {
        //由于更改了weibo的同步方式，这里直接为空就行
    }

    @Override
    protected void executeChild() throws Exception {
        //由于更改了weibo的同步方式，这里直接为空就行
    }

    public static void main(String[] args) {
        try {
//            BaseSync sync = SyncFactory.buildSyncObj(SyncFactory.SyncType.WEIBO);
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            Date start = sdf.parse("2016-07-01 00:00:00");
//            Date end = new Date();
//
//            sync.execute(0, end.getTime());
//            sync.cleanUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
