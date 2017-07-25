package com.dt.mig.sync.words;

import com.ds.dbamp.core.base.entity.table.sync.SyncFilter;
import com.ds.dbamp.core.dao.db.SyncFilterDao;
import com.ds.dbamp.core.utils.SpringBeanFactory;
import com.dt.mig.sync.base.MigSyncConsts;
import com.yeezhao.commons.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abel.chan on 16/11/20.
 */
public class FilterWords implements IWords {

    private static final Logger LOG = LoggerFactory.getLogger(KeyWords.class);

    protected Map<String, List<String[]>> filterWords = null;
    protected Map<String, String> originFilterWords = null;

    public FilterWords() {
        originFilterWords = new HashMap<String, String>();
        filterWords = new HashMap<String, List<String[]>>();
    }

    private static FilterWords filterWordsObj;

    public static FilterWords getInstance() {
        if (filterWordsObj == null) {
            synchronized (KeyWords.class) {
                if (filterWordsObj == null) {
                    filterWordsObj = new FilterWords();
                    filterWordsObj.setUp();
                }
            }
        }
        return filterWordsObj;
    }

    public static void updateFilterwords() {
        filterWordsObj = new FilterWords();
        filterWordsObj.setUp();
    }

    @Override
    public void setUp() {
        try {
            buildFilterWords();
            LOG.info("初始化过滤词完毕:{}", originFilterWords.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, List<String[]>> getWords() {
        return filterWords;
    }

    @Override
    public Map<String, String> getOriginWords() {
        return originFilterWords;
    }

    /**
     * 计算活动的关键词列表
     *
     * @return
     * @throws Exception
     */
    private void buildFilterWords() throws Exception {
        SyncFilterDao syncFilterDao = SpringBeanFactory.getBean(SyncFilterDao.class);

        List<SyncFilter> filters = syncFilterDao.selectAll();
        if (filters != null) {
            for (SyncFilter filter : filters) {
                if (filter != null) {
                    String keyword = filter.getFilterword();
                    String name = filter.getName();
                    if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                        originFilterWords.put(name, keyword);
                        filterWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
                    }
                }
            }
        }
    }

    /**
     * 计算关键词之间的 and 关系
     *
     * @param words
     * @return
     * @throws Exception
     */
    private List<String[]> buildWords(String[] words) throws Exception {

        List<String[]> keyWords = new ArrayList<String[]>();
        if (words != null) {
            for (String keyword : words) {
                keyWords.add(keyword.split(MigSyncConsts.SYMBOL_AND));
            }
        }
        return keyWords;
    }

    @Override
    public String toString() {
        return "FilterWords{" + "originFilterWords=" + originFilterWords + '}';
    }
}
