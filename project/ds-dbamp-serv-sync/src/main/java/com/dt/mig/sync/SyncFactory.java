package com.dt.mig.sync;

import com.dt.mig.sync.newsforum.NewsForumSync;
import com.dt.mig.sync.questionAnswer.QuestionAnswerSync;
import com.dt.mig.sync.weibo.WeiboSync;
import com.dt.mig.sync.weiboComment.WeiboCommentSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * Created by abel.chan on 16/11/20.
 */
public class SyncFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SyncFactory.class);

    public static BaseReader buildSyncObj(SyncType type) {
        BaseReader syncObj = null;
//        if (SyncType.WEIBO == type) {
//            syncObj = new WeiboSync();
//        } else
        if (SyncType.NEWS_FORUM == type) {
            syncObj = new NewsForumSync();
        } else if (SyncType.WEIBO_COMMENT == type) {
            syncObj = new WeiboCommentSync();
        } else if (SyncType.NEWS_QUESTION_ANSWER == type) {
            syncObj = new QuestionAnswerSync();
        } else {
            LOG.error("新建的同步类型不匹配!" + type);
            System.exit(0);
        }

        if (syncObj != null) {
            syncObj.setUp();//条用setup初始化参数
        }

        return syncObj;
    }

    public enum SyncType implements Serializable {
//                WEIBO("weibo"),
        WEIBO_COMMENT("weiboComment"), NEWS_FORUM("newsForum"), NEWS_QUESTION_ANSWER("newsQuestionAnswer");


        private String name;

        private SyncType(String name) {
            this.name = name;
        }

        public static SyncType fromSyncMode(String type) {
//            if (type.equals(WEIBO.name))
//                return WEIBO;
//            else
            if (type.equals(WEIBO_COMMENT.name)) return WEIBO_COMMENT;
            else if (type.equals(NEWS_FORUM.name)) return NEWS_FORUM;
            else if (type.equals(NEWS_QUESTION_ANSWER.name)) return NEWS_QUESTION_ANSWER;
            else return null;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args != null && args.length == 3) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            long timeStart = sdf.parse(args[0]).getTime();
            long timeEnd = sdf.parse(args[1]).getTime();
            SyncType syncType = SyncType.fromSyncMode(args[2]);
            SyncFactory syncObj = new SyncFactory();
            BaseReader baseReader = syncObj.buildSyncObj(syncType);
            baseReader.execute(timeStart, timeEnd);
            baseReader.cleanUp();
        } else {
            System.out.println("please input args: starttime 、endtime、syncType!");
        }
    }
}
