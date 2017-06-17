package com.datastory.banyan.newsforum.kafka.processor;

import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.kafka.ToESKafkaProtocol;
import com.datastory.banyan.newsforum.doc.NFCmtHb2ESDocMapper;
import com.datastory.banyan.newsforum.es.NewsForumCmtESWriter;
import com.datastory.banyan.newsforum.hbase.NewsForumCmtHBaseReader;
import com.datastory.commons.kafka.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Date;

/**
 * com.datastory.banyan.newsforum.kafka.processor.NewsForumCmtToESProcessor
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumCmtToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return NewsForumCmtHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
        return new NFCmtHb2ESDocMapper(p).map();
    }

    @Override
    public ESWriterAPI getESWriter() {
//        return NewsForumCmtEsShardsWriter.getInstance();
        return NewsForumCmtESWriter.getInstance();
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        ToESKafkaProtocol protocol = new ToESKafkaProtocol();
        protocol.setPk("cd10290cd9073698711a0e05a1d40558");
        protocol.setTable("DS_BANYAN_NEWSFORUM_COMMENT_TEST");
        protocol.setPublish_date(DateUtils.getCurrentTimeStr());
        protocol.setUpdate_date(DateUtils.getCurrentTimeStr());

        ToESKafkaProtocol protocol1 = new ToESKafkaProtocol();
        protocol1.setPk("f9855081c3cc4ccd5f33a8e5f7d40ace");
        protocol1.setTable("DS_BANYAN_NEWSFORUM_COMMENT_TEST");
        protocol1.setPublish_date(DateUtils.getCurrentTimeStr());
        protocol1.setUpdate_date(DateUtils.getCurrentTimeStr());

        NewsForumCmtToESProcessor processor = new NewsForumCmtToESProcessor();
        processor.process(protocol);
        processor.process(protocol1);
        processor.flush();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
