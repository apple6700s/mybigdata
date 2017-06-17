package com.datastory.banyan.newsforum.kafka.processor;

import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.newsforum.doc.NFPostHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumPostESWriter;
import com.datastory.banyan.newsforum.hbase.NewsForumPostHBaseReader;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.newsforum.kafka.processor.NewsForumPostToESProcessor
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumPostToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return NewsForumPostHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
        return new NFPostHb2ESDocMapper(p).map();
    }

    @Override
    public ESWriterAPI getESWriter() {
//        return NewsForumPostEsShardsWriter.getInstance();
        return NewsForumPostESWriter.getInstance();
    }
}
