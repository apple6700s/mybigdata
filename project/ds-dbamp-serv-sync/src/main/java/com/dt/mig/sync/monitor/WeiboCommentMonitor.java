package com.dt.mig.sync.monitor;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * Created by abel.chan on 17/1/27.
 */
public class WeiboCommentMonitor extends BaseMonitor {

    public WeiboCommentMonitor() {
        this.esIndex = MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX;
        this.exType = MigSyncConsts.ES_WEIBO_COMMENT_CHILD_WRITE_TYPE;
        this.timeAggField = MigSyncConsts.ES_WEIBO_COMMENT_COMMENT_COMMENT_DATE;
        this.timeStampField = MigSyncConsts.ES_WEIBO_COMMENT_COMMENT_COMMENT_TIME;
        this.dataType = DataMonitorFactory.DataMonitorType.WEIBO_COMMENT.getName();
    }

    @Override
    public void execute() {
        super.execute();
    }
}
