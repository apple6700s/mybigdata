package com.datastory.banyan.weibo.es;

import com.datastory.banyan.es.BanyanESWriter;

/**
 * com.datastory.banyan.weibo.es.CmtWbESWriter
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class CmtESWriter extends BanyanESWriter {
    private static final String INDEX = "ds-banyan-weibo-comment-index";
    private static volatile CmtESWriter _singleton = null;

    public static CmtESWriter getInstance() {
        if (_singleton == null) {
            synchronized (CmtESWriter.class) {
                if (_singleton == null) {
                    _singleton = new CmtESWriter();
                }
            }
        }
        return _singleton;
    }

    public static CmtESWriter getInstance(String index) {
        if (_singleton == null) {
            synchronized (CmtESWriter.class) {
                if (_singleton == null) {
                    _singleton = new CmtESWriter(index);
                }
            }
        }
        return _singleton;
    }

    public CmtESWriter(String index) {
        super(index, "comment");
    }

    public CmtESWriter() {
        super(INDEX, "comment");
    }

    public CmtESWriter(int bulkNum) {
        super(INDEX, "comment", bulkNum);
    }
}
