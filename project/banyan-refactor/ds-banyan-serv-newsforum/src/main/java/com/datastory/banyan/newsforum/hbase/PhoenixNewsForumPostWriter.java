package com.datastory.banyan.newsforum.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;
import com.datastory.banyan.newsforum.kafka.NewsForumPostKafkaProducer;

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
        super.init();
        this.setEsWriterHook(NewsForumPostESWriter.getInstance(), NFPostHb2ESDocMapper.class);
//        this.setEsWriterHook(NewsForumPostESWriter.createSimpleWriterFactory(cacheSize), NFPostHb2ESDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_LONGTEXT_POST_TBL);
    }
}
