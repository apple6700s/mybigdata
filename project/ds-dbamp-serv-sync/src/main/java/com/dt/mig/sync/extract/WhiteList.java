package com.dt.mig.sync.extract;

import com.ds.dbamp.core.base.entity.table.sync.KeywordWhiteList;
import com.ds.dbamp.core.dao.db.KeywordWhiteListDao;
import com.ds.dbamp.core.utils.SpringBeanFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 16/12/21.
 */
public class WhiteList extends ArrayList<String> {

    private static final Logger LOG = LoggerFactory.getLogger(WhiteList.class);

//    private static WhiteList whiteList;

    public WhiteList() {
        init();//初始化白名单
    }

    private void init() {
        try {
            KeywordWhiteListDao keywordWhiteListDao = SpringBeanFactory.getBean(KeywordWhiteListDao.class);

            List<KeywordWhiteList> whiteLists = keywordWhiteListDao.selectAll();
            for (KeywordWhiteList keywordWhite : whiteLists) {
                this.add(keywordWhite.getWord());
            }
            LOG.info("白名单:" + this.toString());
        } catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
    }

//    public static WhiteList getInstance() {
//        if (whiteList == null) {
//            synchronized (WhiteList.class) {
//                if (whiteList == null) {
//                    whiteList = new WhiteList();
//                }
//            }
//        }
//        return whiteList;
//    }
}
