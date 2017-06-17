package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;

/**
 * com.datastory.banyan.newsforum.hbase.NewsForumPostHBaseWriter
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class NewsForumPostHBaseWriter extends BanyanRFieldPutter {
    private static volatile NewsForumPostHBaseWriter _singleton = null;

    public static NewsForumPostHBaseWriter getInstance() {
        if (_singleton == null)
            synchronized (NewsForumPostHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumPostHBaseWriter();
                }
            }
        return _singleton;
    }

    public NewsForumPostHBaseWriter() {
        super(Tables.table(Tables.PH_LONGTEXT_POST_TBL));
    }

    protected void init() {
        super.init();
        this.setEsWriterHook(NewsForumPostESWriter.getInstance(), NFPostHb2ESDocMapper.class);
    }
}
