package com.dt.mig.sync.words;

import com.ds.dbamp.core.base.entity.table.product.Product;
import com.ds.dbamp.core.base.entity.table.special.SpecialTask;
import com.ds.dbamp.core.dao.db.ProductDao;
import com.ds.dbamp.core.dao.db.SpecialTaskDao;
import com.ds.dbamp.core.utils.SpringBeanFactory;
import com.dt.mig.sync.base.MigSyncConsts;
import com.yeezhao.commons.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by abel.chan on 16/11/20.
 */
public class KeyWords implements IWords {

    private static final Logger LOG = LoggerFactory.getLogger(KeyWords.class);

    //将 and 与 or拆开。 string【】保存and关系,List元素保存or 关系。
    protected Map<String, List<String[]>> keyWords = null;

    //存储关键词之间的关系,
    protected Map<String, String> originKeyWords = null;

    public KeyWords() {
        originKeyWords = new HashMap<String, String>();
        keyWords = new HashMap<String, List<String[]>>();
    }

    private static KeyWords keyWordsObj;

    public static KeyWords getInstance() {
        if (keyWordsObj == null) {
            synchronized (KeyWords.class) {
                if (keyWordsObj == null) {
                    keyWordsObj = new KeyWords();
                    keyWordsObj.setUp();
                }
            }
        }
        return keyWordsObj;
    }

    public static void updateKeywords() {
        keyWordsObj = new KeyWords();
        keyWordsObj.setUp();
    }

    public Set<String> getAllKeys() {
        return originKeyWords.keySet();
    }

    public String getOriginValue(String key) {
        if (originKeyWords != null && originKeyWords.containsKey(key)) {
            return originKeyWords.get(key);
        }
        return null;
    }


    public void setUp() {

        try {
            buildProductKeyWords();
            buildSpecialTaskKeyWords();
            LOG.info("初始化关键词完毕:{}", originKeyWords.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }


    @Override
    public Map<String, List<String[]>> getWords() {
        return keyWords;
    }


    @Override
    public Map<String, String> getOriginWords() {
        return originKeyWords;
    }


    /**
     * 计算关键词列表
     *
     * @return
     * @throws Exception
     */
    public void buildKeyWords(Map<String, String> keywordMap) throws Exception {
        for (Map.Entry<String, String> entry : keywordMap.entrySet()) {
            String keyword = entry.getValue();
            String name = entry.getKey();
            if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                originKeyWords.put(name, keyword);
                keyWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
            }
        }
    }

    /**
     * 计算品牌的关键词列表
     *
     * @return
     * @throws Exception
     */
    public void buildProductKeyWords() throws Exception {
        ProductDao productDao = SpringBeanFactory.getBean(ProductDao.class);

        List<Product> products = productDao.selectAllProducts();
        if (products != null) {
            for (Product product : products) {
                if (product != null) {
                    String keyword = product.getKeyword();
                    String name = product.getName();
                    if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                        originKeyWords.put(name, keyword);
                        keyWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
                    }
                }
            }
        }
    }

    /**
     * 计算新品牌的关键词列表
     *
     * @return
     * @throws Exception
     */
    public void buildProductKeyWords(List<Integer> pids) throws Exception {

        if (pids != null && pids.size() > 0) {
            ProductDao productDao = SpringBeanFactory.getBean(ProductDao.class);
            for (Integer pid : pids) {
                Product product = productDao.selectOne(pid);
                if (product != null) {
                    String keyword = product.getKeyword();
                    String name = product.getName();
                    if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                        originKeyWords.put(name, keyword);
                        keyWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
                    }
                }
            }
        }
    }


    /**
     * 计算活动的关键词列表
     *
     * @return
     * @throws Exception
     */
    private void buildSpecialTaskKeyWords() throws Exception {
        SpecialTaskDao specialTaskDao = SpringBeanFactory.getBean(SpecialTaskDao.class);

        List<SpecialTask> specialTasks = specialTaskDao.selectAll();
        if (specialTasks != null) {
            for (SpecialTask task : specialTasks) {
                if (task != null) {
                    String keyword = task.getKeyword();
                    String name = task.getName();
                    if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                        originKeyWords.put(name, keyword);
                        keyWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
                    }
                }
            }
        }
    }

    /**
     * 计算活动的关键词列表
     *
     * @return
     * @throws Exception
     */
    public void buildSpecialTaskKeyWords(List<Integer> tids) throws Exception {
        if (tids != null && tids.size() > 0) {
            SpecialTaskDao specialTaskDao = SpringBeanFactory.getBean(SpecialTaskDao.class);
            for (Integer tid : tids) {
                SpecialTask task = specialTaskDao.selectOne(tid);
                if (task != null) {
                    String keyword = task.getKeyword();
                    String name = task.getName();
                    if (!StringUtil.isNullOrEmpty(keyword) && !StringUtil.isNullOrEmpty(name)) {
                        originKeyWords.put(name, keyword);
                        keyWords.put(name, buildWords(keyword.split(MigSyncConsts.SYMBOL_OR)));
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
        return "KeyWords{" + "originKeyWords=" + originKeyWords + '}';
    }
}
