package com.dt.mig.sync;

import com.dt.mig.sync.newsforum.NewsForumUpdate;
import com.dt.mig.sync.questionAnswer.QuestionAnswerUpdate;
import com.dt.mig.sync.wechat.WechatUpdate;
import com.dt.mig.sync.weibo.WeiboUpdate;
import com.dt.mig.sync.weiboComment.WeiboCommentUpdate;
import com.dt.mig.sync.words.KeyWords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by abel.chan on 16/11/20.
 */
public class UpdateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateFactory.class);

    public static BaseReader buildUpdateObj(UpdateType type, List<Integer> newPIds, List<Integer> newTIds) {

        if (!checkCondition(newPIds, newTIds)) {
            LOG.error("参数传递有误,新品牌、新活动不能为空!");
            System.exit(1);
        }

        //添加活动的词
        KeyWords newKeyWords = new KeyWords();
        try {
            newKeyWords.buildProductKeyWords(newPIds);
            newKeyWords.buildSpecialTaskKeyWords(newTIds);
        } catch (Exception e) {
            LOG.error("参数传递有误,新品牌、新活动不能为空!");
            System.exit(1);
        }

        if (newKeyWords.getOriginWords().size() <= 0) {
            LOG.error("新品牌、新活动不能为空!");
            System.exit(1);
        }

        LOG.info("更新新活动、新品牌关键词:" + newKeyWords.toString() + ":::" + type);

        BaseReader obj = null;
        if (UpdateType.WEIBO == type) {
            obj = new WeiboUpdate();
        } else if (UpdateType.NEWS_FORUM == type) {
            obj = new NewsForumUpdate();
        } else if (UpdateType.WEIBO_COMMENT == type) {
            obj = new WeiboCommentUpdate();
        } else if (UpdateType.NEWS_QUESTION_ANSWER == type) {
            obj = new QuestionAnswerUpdate();
        } else if (UpdateType.Wechat == type) {
           obj = new WechatUpdate();
        } else {
            LOG.error("新建的同步类型不匹配!" + type);
            System.exit(0);
        }

        if (obj != null) {
            obj.setUp();//调用setup初始化参数
            obj.setNewKeyWords(newKeyWords);
        }

        return obj;
    }

    public static boolean checkCondition(List<Integer> newPIds, List<Integer> newTIds) {
        if ((newPIds != null && newPIds.size() > 0) || (newTIds != null && newTIds.size() > 0)) {
            return true;
        }
        return false;
    }

    public enum UpdateType {
        WEIBO("weibo"), WEIBO_COMMENT("weiboComment"), NEWS_FORUM("newsForum"), NEWS_QUESTION_ANSWER("newsQuestionAnswer"), Wechat("wechat");

        private String name;

        private UpdateType(String name) {
            this.name = name;
        }

        public static UpdateType fromSyncMode(String type) {
            if (type.equals(WEIBO.name)) return WEIBO;
            else if (type.equals(WEIBO_COMMENT.name)) return WEIBO_COMMENT;
            else if (type.equals(NEWS_FORUM.name)) return NEWS_FORUM;
            else if (type.equals(NEWS_QUESTION_ANSWER.name)) return NEWS_QUESTION_ANSWER;
            else if (type.equals(Wechat.name)) return Wechat;
            else return null;
        }
    }
}
