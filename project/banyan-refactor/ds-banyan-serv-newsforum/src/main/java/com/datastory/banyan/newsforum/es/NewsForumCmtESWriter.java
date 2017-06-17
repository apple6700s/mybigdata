package com.datastory.banyan.newsforum.es;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.es.*;
import com.datastory.banyan.utils.FactoryFunc;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Date;
import java.util.Map;

/**
 * com.datastory.banyan.newsforum.es.NewsForumCmtESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumCmtESWriter extends BanyanESWriter {

    private static volatile NewsForumCmtESWriter _singleton = null;

    public static NewsForumCmtESWriter getInstance() {
        if (_singleton == null)
            synchronized (NewsForumCmtESWriter.class) {
                if (_singleton == null) {
                    _singleton = new NewsForumCmtESWriter();
                }
            }
        return _singleton;
    }

    public NewsForumCmtESWriter() {
        super(Tables.table(Tables.ES_LTEXT_IDX), "comment");
    }

    public static FactoryFunc<ESWriterAPI> createSimpleWriterFactory(final int cacheSize) {
        return new FactoryFunc<ESWriterAPI>() {
            @Override
            public ESWriterAPI create() {
                SimpleEsBulkClient bulkClient = ESClientFactory.createESBulkProcessorClient(
                        Tables.table(Tables.ES_LTEXT_IDX), "comment", cacheSize
                );
                SimpleEsWriter esWriter = new SimpleEsWriter(bulkClient);
                return esWriter;
            }
        };
    }

    @Override
    public void write(Map<String, Object> doc) {
        if (!doc.containsKey("_parent"))
            doc.put("_parent", doc.get("parent_post_id"));
        super.write(doc);
    }

    /**
     * test main
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        NewsForumCmtESWriter writer = NewsForumCmtESWriter.getInstance();
        Params p1 = new Params("id", "cmt1");
        Params p2 = new Params("id", "cmt2");
        Params p3 = new Params("id", "cmt3");

        writer.write(p1);
        writer.write(p2);
        writer.write(p3);
        writer.flush();

        Thread.sleep(3000);

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
