package com.datastory.banyan.newsforum.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Date;

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

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        Params p0 = new Params("id", "00000");
        p0.put("cat_id", (short) 0);
        Params p1 = new Params("id", "00001");
        p1.put("cat_id", (short) 1);
        Params p2 = new Params("id", "00002");
        p2.put("cat_id", (short) 2);

        NewsForumPostESWriter writer = NewsForumPostESWriter.getInstance();
        writer.write(p0);
        writer.write(p1);
        writer.write(p2);
        writer.flush();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
