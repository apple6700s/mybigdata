package com.datastory.banyan.weibo.es;

import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.weibo.es.CmtWbESWriter
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class CmtWbESWriter extends BanyanESWriter {
    private static final String INDEX = "ds-banyan-weibo-comment-index";
    private static volatile CmtWbESWriter _singleton = null;

    public static CmtWbESWriter getInstance() {
        if (_singleton == null) {
            synchronized (CmtWbESWriter.class) {
                if (_singleton == null) {
                    _singleton = new CmtWbESWriter();
                }
            }
        }
        return _singleton;
    }

    public static CmtWbESWriter getInstance(String index) {
        if (_singleton == null) {
            synchronized (CmtWbESWriter.class) {
                if (_singleton == null) {
                    _singleton = new CmtWbESWriter(index);
                }
            }
        }
        return _singleton;
    }

    public CmtWbESWriter(String index) {
        super(index, "weibo");
    }

    public CmtWbESWriter() {
        super(INDEX, "weibo");
    }

    public CmtWbESWriter(int bulkNum) {
        super(INDEX, "weibo", bulkNum);
    }
}
