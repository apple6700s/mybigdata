package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;

import java.util.Arrays;

/**
 * com.datastory.banyan.newsforum.hbase.PhoenixNewsForumCmtWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PhoenixNewsForumCmtWriter extends PhoenixWriter {
    private static volatile PhoenixNewsForumCmtWriter _singleton = null;

    public static PhoenixNewsForumCmtWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixNewsForumCmtWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixNewsForumCmtWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixNewsForumCmtWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixNewsForumCmtWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixNewsForumCmtWriter(num);
                }
            }
        return _singleton;
    }


    public PhoenixNewsForumCmtWriter() {
        this(1000);
    }

    public PhoenixNewsForumCmtWriter(int cacheSize) {
        super(cacheSize);
    }

    @Override
    protected void init() {
        setFields(Arrays.asList(
                "pk", "update_date", "publish_date", "parent_post_id", "cat_id",
                "title", "is_main_post", "content", "author", "url", "page_id",
                "site_id", "site_name", "source", "sentiment", "keywords", "fingerprint",
                "is_ad", "is_robot", "taskId", "view_cnt", "review_cnt", "like_cnt", "dislike_cnt"
        ));
        if (RhinoETLConfig.getInstance().getBoolean("enable.es.writer", true))
            this.setEsWriterHook(NewsForumCmtESWriter.getInstance(), NFCmtHb2ESDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
    }
}
