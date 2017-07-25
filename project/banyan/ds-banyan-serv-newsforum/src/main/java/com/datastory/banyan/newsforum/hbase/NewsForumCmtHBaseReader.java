package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;

/**
 * com.datastory.banyan.newsforum.hbase.NewsForumCmtHBaseReader
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumCmtHBaseReader extends HBaseReader {
    private static final String TABLE = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);

    private static volatile NewsForumCmtHBaseReader _singleton = null;

    public static NewsForumCmtHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (NewsForumCmtHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumCmtHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
