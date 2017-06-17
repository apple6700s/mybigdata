package com.datastory.banyan.weibo.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.weibo.es.CmtWbESWriter
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class RhinoCmtWbESWriter extends BanyanESWriter {
    private static volatile RhinoCmtWbESWriter _singleton = null;

    public static RhinoCmtWbESWriter getInstance() {
        if (_singleton == null) {
            synchronized (RhinoCmtWbESWriter.class) {
                if (_singleton == null) {
                    _singleton = new RhinoCmtWbESWriter();
                }
            }
        }
        return _singleton;
    }

    public static RhinoCmtWbESWriter getInstance(String index) {
        if (_singleton == null) {
            synchronized (RhinoCmtWbESWriter.class) {
                if (_singleton == null) {
                    _singleton = new RhinoCmtWbESWriter(index);
                }
            }
        }
        return _singleton;
    }

    public RhinoCmtWbESWriter(String index) {
        super(index, "weibo");
    }

    public RhinoCmtWbESWriter() {
        super(Tables.table(Tables.ES_WBCMT_IDX), "weibo");
    }

    public RhinoCmtWbESWriter(int bulkNum) {
        super(Tables.table(Tables.ES_WBCMT_IDX), "weibo", bulkNum);
    }
}
