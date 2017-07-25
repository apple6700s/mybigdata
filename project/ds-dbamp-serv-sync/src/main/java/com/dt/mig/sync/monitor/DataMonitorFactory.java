package com.dt.mig.sync.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by abel.chan on 17/1/27.
 */
public class DataMonitorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataMonitorFactory.class);

    public static List<IMonitor> getAllDataMonitorObj() {
        return Arrays.asList(getNewsfourmDataMonitorObj(), getWeiboCommentDataMonitorObj(), getWeiboDataMonitorObj());
    }

    public static IMonitor getDataMonitor(DataMonitorType type) {
        if (DataMonitorType.WEIBO == type) {
            return getWeiboDataMonitorObj();
        } else if (DataMonitorType.NEWS_FORUM == type) {
            return getNewsfourmDataMonitorObj();
        } else if (DataMonitorType.WEIBO_COMMENT == type) {
            return getWeiboCommentDataMonitorObj();
        } else {
            LOG.error("新建的同步类型不匹配!" + type);
            System.exit(0);
        }
        return null;
    }

    private static IMonitor getWeiboDataMonitorObj() {
        return new WeiboMonitor();
    }

    private static IMonitor getWeiboCommentDataMonitorObj() {
        return new WeiboCommentMonitor();
    }

    private static IMonitor getNewsfourmDataMonitorObj() {
        return new NewsForumMonitor();
    }

    public enum DataMonitorType {
        WEIBO("weibo"), WEIBO_COMMENT("weiboComment"), NEWS_FORUM("newsForum");

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        private DataMonitorType(String name) {
            this.name = name;
        }

        public static DataMonitorType fromMonitorMode(String type) {
            if (type.equals(WEIBO.name)) return WEIBO;
            else if (type.equals(WEIBO_COMMENT.name)) return WEIBO_COMMENT;
            else if (type.equals(NEWS_FORUM.name)) return NEWS_FORUM;
            else return null;
        }
    }
}
