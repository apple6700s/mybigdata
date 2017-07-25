package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;

import java.util.Arrays;

/**
 * com.datastory.banyan.newsforum.hbase.PhoenixNewsForumPostWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class PhoenixNewsForumPostWriter extends PhoenixWriter {
    private static volatile PhoenixNewsForumPostWriter _singleton = null;

    public static PhoenixNewsForumPostWriter getInstance() {
        if (_singleton == null)
            synchronized (PhoenixNewsForumPostWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixNewsForumPostWriter();
                }
            }
        return _singleton;
    }

    public static PhoenixNewsForumPostWriter getInstance(int num) {
        if (_singleton == null)
            synchronized (PhoenixNewsForumPostWriter.class) {
                if (_singleton == null) {
                    _singleton = new PhoenixNewsForumPostWriter(num);
                }
            }
        return _singleton;
    }

    public PhoenixNewsForumPostWriter() {
    }

    public PhoenixNewsForumPostWriter(int cacheSize) {
        super(cacheSize);
    }

    @Override
    protected void init() {
        setFields(Arrays.asList(
                "pk", "update_date", "publish_date", "cat_id", "title",
                "content", "all_content", "author", "view_cnt", "review_cnt",
                "url", "site_id", "site_name", "source", "is_digest", "is_hot",
                "is_top", "is_recom", "sentiment", "keywords", "fingerprint",
                "is_ad", "is_robot", "introduction", "origin_label", "same_html_count",
                "taskId", "like_cnt", "dislike_cnt"
        ));
        if (RhinoETLConfig.getInstance().getBoolean("enable.es.writer", true))
            this.setEsWriterHook(NewsForumPostESWriter.getInstance(), NFPostHb2ESDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_LONGTEXT_POST_TBL);
    }
}
