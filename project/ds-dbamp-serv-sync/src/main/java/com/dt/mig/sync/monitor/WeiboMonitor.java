package com.dt.mig.sync.monitor;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * Created by abel.chan on 17/1/27.
 */
public class WeiboMonitor extends BaseMonitor {

    public WeiboMonitor() {
        this.esIndex = MigSyncConsts.ES_WEIBO_WRITER_INDEX;
        this.exType = MigSyncConsts.ES_WEIBO_CHILD_WRITE_TYPE;
        this.timeAggField = MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME_DATE;
        this.timeStampField = MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME;
        this.dataType = DataMonitorFactory.DataMonitorType.WEIBO.getName();
    }

    @Override
    public void execute() {
        super.execute();
    }
}
