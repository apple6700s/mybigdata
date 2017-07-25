package com.dt.mig.sync.words;

import com.ds.dbamp.core.dao.db.DeletedDocInfoDao;
import com.ds.dbamp.core.utils.SpringBeanFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 16/12/9.
 */
public class FilterDoc {

    private static final Logger LOG = LoggerFactory.getLogger(FilterDoc.class);

    private static FilterDoc doc;

    //public Map<String, List<String>> filterMap = new HashMap();

    public final static String WEIBO = "weibo";

    public final static String FORUM = "forum";

    public final static String NEWS = "news";

    public final static String TIEBA = "tieba";

    public final static String NEWSFORUM = "newsforum";

    private FilterDoc() {

    }

    public static FilterDoc getInstance() {
        if (doc == null) {
            synchronized (FilterDoc.class) {
                if (doc == null) {
                    doc = new FilterDoc();
                }
            }
        }
        return doc;
    }

    public List<String> getWeiboDocId() {
        LOG.info("开始获取需要过滤的weibo的doc ID");
        try {
            List<String> list = initWeibo();
            LOG.info("开始获取需要过滤的weibo的doc ID.size:{},content:", list.size(), list != null ? list.toString() : "");
            return list;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return new ArrayList<>();
    }

    public List<String> getNewsForumDocId() {
        LOG.info("开始获取需要过滤的newsForums的doc ID");
        try {
            List<String> list = initNewsForum();
            LOG.info("开始获取需要过滤的newsForums的doc ID.size:{},content:", list.size(), list != null ? list.toString() : "");
            return list;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return new ArrayList<>();
    }

    private List<String> initWeibo() throws Exception {
        List<String> ret = getDoc(WEIBO);
        if (ret == null) {
            ret = new ArrayList();
        }
        return ret;
    }

    private List<String> initNewsForum() throws Exception {
        List<String> ret = new ArrayList();
        {
            List<String> list = getDoc(NEWS);
            if (list != null) {
                ret.addAll(list);
            }
        }
        {
            List<String> list = getDoc(FORUM);
            if (list != null) {
                ret.addAll(list);
            }
        }
        {
            List<String> list = getDoc(TIEBA);
            if (list != null) {
                ret.addAll(list);
            }
        }
        if (ret == null) {
            ret = new ArrayList();
        }

        return ret;
    }

    private List<String> getDoc(String type) throws Exception {
        DeletedDocInfoDao docDao = SpringBeanFactory.getBean(DeletedDocInfoDao.class);
        return docDao.selectDocIdByType(type);
    }

}
