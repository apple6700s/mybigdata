package com.datastory.banyan.newsforum.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;

/**
 * com.datastory.banyan.newsforum.es.NewsForumPostESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumPostESWriter extends BanyanESWriter {

    private static volatile NewsForumPostESWriter _singleton = null;

    public static NewsForumPostESWriter getInstance() {
        if (_singleton == null)
            synchronized (NewsForumPostESWriter.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumPostESWriter();
                }
            }
        return _singleton;
    }

    public NewsForumPostESWriter() {
        super(Tables.table(Tables.ES_LTEXT_IDX), "post");
    }

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory.createESBulkProcessorClient(
                        Tables.table(Tables.ES_LTEXT_IDX), "post", cacheSize
                );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }
}
