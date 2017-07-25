package com.dt.mig.sync.monitor;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * Created by abel.chan on 17/1/27.
 */
public class NewsForumMonitor extends BaseMonitor {

    public NewsForumMonitor() {
        this.esIndex = MigSyncConsts.ES_NEWS_FORUM_WRITER_INDEX;
        this.exType = MigSyncConsts.ES_NEWS_FORUM_CHILD_WRITE_TYPE;
        this.timeAggField = MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_DATE_DATE;
        this.timeStampField = MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_DATE;
        this.dataType = DataMonitorFactory.DataMonitorType.NEWS_FORUM.getName();
    }

    @Override
    public void execute() {
        super.execute();
    }
}
