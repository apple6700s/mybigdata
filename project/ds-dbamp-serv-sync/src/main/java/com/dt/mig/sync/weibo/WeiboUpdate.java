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
public class WeiboUpdate extends BaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboUpdate.class);

    public WeiboUpdate() {
        super();
    }

    //需要build的字段
    public static final String[] SEARCH_ES_FILEDS = new String[]{MigSyncConsts.ES_WEIBO_WEIBO_CONTENT};

    @Override
    public void execute(long start, long end) throws Exception {

        if (newKeyWords == null) {
            LOG.error("需要更新的关键词不能为空!");
            System.exit(1);
        }

        //for (String field : SEARCH_ES_FILEDS) {

        //currentSearchEsField = field;//当前需要查询的字段
        //LOG.info("开始同步满足关键词的{}字段的es记录,时间段: {} - {}", currentSearchEsField, new Date(start), new Date(end));

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
        //由于数据量太大,所以一次只使用一个查询字段。并且使用新的关键词进行查询
        return CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), newKeyWords, filterWords, FilterDoc.getInstance().getWeiboDocId(), MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);
    }


    @Override
    protected void executeParent() throws Exception {
        //由于更改了weibo的同步方式，这里直接为空就行
    }

    @Override
    protected void executeChild() throws Exception {
        //由于更改了weibo的同步方式，这里直接为空就行
    }
}
