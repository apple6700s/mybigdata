package com.dt.mig.sync.monitor;

import com.ds.dbamp.core.base.entity.table.sys.DataMonitor;
import com.ds.dbamp.core.dao.db.DataMonitorDao;
import com.ds.dbamp.core.utils.SpringBeanFactory;
import com.dt.mig.sync.es.CommonAggBuilder;
import com.dt.mig.sync.es.CommonReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by abel.chan on 17/1/27.
 */
public class BaseMonitor implements IMonitor {


    private static final Logger log = LoggerFactory.getLogger(BaseMonitor.class);

    protected String timeStampField = "";
    protected String timeAggField = "";
    protected String esIndex = "";
    protected String exType = "";
    protected String dataType = "";
    protected int TimeSegment = 7;

    public void execute() {

        try {
            CommonReader reader = CommonReader.getInstance();

            long current = System.currentTimeMillis();//当前时间毫秒数
            //N天前的零点零分零秒的毫秒数
            Date yesterdayZero = new Date(current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset() - (1000 * 3600 * 24));
            Date start = DateUtils.addDays(yesterdayZero, -1 * TimeSegment);

            //昨天23点59分59秒的毫秒数)
            Date end = new Date(current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset() - (1));

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            QueryBuilder rangeQuery = QueryBuilders.rangeQuery(timeStampField).from(start.getTime()).to(end.getTime());
            queryBuilder.must(rangeQuery);

            AggregationBuilder aggregationBuilder = CommonAggBuilder.buildTermsAgg(timeAggField, TimeSegment, false);
            Aggregations aggResult = reader.aggSearch(esIndex, exType, queryBuilder, aggregationBuilder);
            Terms termsAggResult = aggResult.get(timeAggField);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String targetTimeFormat = sdf.format(end);
            int targetTimeSize = 0;
            int maxSize = 0;
            int minSize = Integer.MAX_VALUE;
            int otherTotalCount = 0;
            int otherTotalSize = 0;

            for (Terms.Bucket entry : termsAggResult.getBuckets()) {
                String key = entry.getKeyAsString();
                if (StringUtils.isNotEmpty(key)) {
                    int docCount = (int) entry.getDocCount();
                    log.info(key + ":::" + docCount);
                    if (targetTimeFormat.equals(key)) {
                        targetTimeSize = docCount;
                    } else {
                        otherTotalSize += docCount;
                        otherTotalCount++;
                        if (maxSize < docCount) {
                            maxSize = docCount;
                        }
                        if (minSize > docCount) {
                            minSize = docCount;
                        }
                    }
                }
            }
            log.info("targetTimeSize:" + targetTimeSize);
            log.info("maxSize:" + maxSize);
            log.info("minSize:" + minSize);
            log.info("otherTotalCount:" + otherTotalCount);
            log.info("otherTotalSize:" + otherTotalSize);
            DataMonitorDao dataMonitorDao = SpringBeanFactory.getBean(DataMonitorDao.class);
            int id = getDataMonitorId(dataMonitorDao, yesterdayZero, dataType);
            DataMonitor monitor = getDataMonitorEntity(targetTimeSize, maxSize, minSize, otherTotalSize, otherTotalCount, dataType, yesterdayZero);
            if (id == -1) {
                insert(dataMonitorDao, monitor);
            } else {
                monitor.setId(id);
                update(dataMonitorDao, monitor);
            }
            reader.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public int getDataMonitorId(DataMonitorDao dataMonitorDao, Date createTime, String dataType) {
        try {
            DataMonitor condition = new DataMonitor();
            condition.setCreateTime(createTime);
            condition.setDataType(dataType);
            List<DataMonitor> lists = dataMonitorDao.selectByCondition(condition);
            if (lists != null && lists.size() > 0) {
                return lists.get(0).getId();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return -1;
    }

    public DataMonitor getDataMonitorEntity(int dataSize, int maxSize, int minSize, int otherTotalSize, int otherTotalCount, String dateType, Date createTime) {
        DataMonitor monitor = new DataMonitor();
        monitor.setAvgSize(otherTotalCount != 0 ? new BigDecimal(otherTotalSize * 1.0 / otherTotalCount).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue() : 0);
        monitor.setCreateTime(createTime);
        monitor.setDataSize(dataSize);
        monitor.setDataType(dateType);
        monitor.setMaxSize(maxSize);
        monitor.setMinSize(minSize);
        return monitor;
    }

    public void insert(DataMonitorDao dataMonitorDao, DataMonitor monitor) throws Exception {
        if (monitor != null) {
            dataMonitorDao.insert(monitor);
        }
    }

    public void update(DataMonitorDao dataMonitorDao, DataMonitor monitor) throws Exception {
        if (monitor != null) {
            dataMonitorDao.update(monitor);
        }
    }

}
