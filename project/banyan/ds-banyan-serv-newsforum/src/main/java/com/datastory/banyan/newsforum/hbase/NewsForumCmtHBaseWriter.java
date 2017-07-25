package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;

/**
 * com.datastory.banyan.newsforum.hbase.NewsForumCmtHBaseWriter
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class NewsForumCmtHBaseWriter extends BanyanRFieldPutter {
    private static volatile NewsForumCmtHBaseWriter _singleton = null;

    public static NewsForumCmtHBaseWriter getInstance() {
        if (_singleton == null)
            synchronized (NewsForumCmtHBaseWriter.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumCmtHBaseWriter();
                }
            }
        return _singleton;
    }

    public NewsForumCmtHBaseWriter() {
        super(Tables.table(Tables.PH_LONGTEXT_CMT_TBL));
    }

    protected void init() {
        super.init();
        this.setEsWriterHook(NewsForumCmtESWriter.getInstance(), NFCmtHb2ESDocMapper.class);
    }
}
