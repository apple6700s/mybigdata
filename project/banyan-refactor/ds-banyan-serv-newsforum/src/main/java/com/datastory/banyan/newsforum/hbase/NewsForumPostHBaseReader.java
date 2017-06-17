package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;

/**
 * com.datastory.banyan.newsforum.hbase.NewsForumPostHBaseReader
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumPostHBaseReader extends HBaseReader {
    private static final String TABLE = Tables.table(Tables.PH_LONGTEXT_POST_TBL);

    private static volatile NewsForumPostHBaseReader _singleton = null;

    public static NewsForumPostHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (NewsForumPostHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumPostHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return TABLE;
    }
}
