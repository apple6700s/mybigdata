package com.datastory.banyan.newsforum.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.PKSpoutKafkaProducer;

/**
 * com.datastory.banyan.newsforum.kafka.NewsForumPostKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class NewsForumCmtKafkaProducer extends PKSpoutKafkaProducer {
    public NewsForumCmtKafkaProducer() {
        super(Tables.table(Tables.PH_LONGTEXT_CMT_TBL), Tables.table(Tables.KFK_PK_LT_TP));
    }
}
