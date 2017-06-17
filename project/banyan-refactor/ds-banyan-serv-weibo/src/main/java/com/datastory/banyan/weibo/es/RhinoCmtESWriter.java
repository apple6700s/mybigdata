package com.datastory.banyan.weibo.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.weibo.es.CmtWbESWriter
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class RhinoCmtESWriter extends BanyanESWriter {
    private static volatile RhinoCmtESWriter _singleton = null;

    public static RhinoCmtESWriter getInstance() {
        if (_singleton == null) {
            synchronized (RhinoCmtESWriter.class) {
                if (_singleton == null) {
                    _singleton = new RhinoCmtESWriter();
                }
            }
        }
        return _singleton;
    }

    public static RhinoCmtESWriter getInstance(String index) {
        if (_singleton == null) {
            synchronized (RhinoCmtESWriter.class) {
                if (_singleton == null) {
                    _singleton = new RhinoCmtESWriter(index);
                }
            }
        }
        return _singleton;
    }

    public RhinoCmtESWriter(String index) {
        super(index, "comment");
    }

    public RhinoCmtESWriter() {
        super(Tables.table(Tables.ES_WBCMT_IDX), "comment");
    }

    public RhinoCmtESWriter(int bulkNum) {
        super(Tables.table(Tables.ES_WBCMT_IDX), "comment", bulkNum);
    }
}
