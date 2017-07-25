package com.datastory.banyan.weibo.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.PKSpoutKafkaProducer;

/**
 * com.datastory.banyan.weibo.kafka.WeiboContentKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class WeiboContentKafkaProducer extends PKSpoutKafkaProducer {

    private static volatile WeiboContentKafkaProducer _singleton = null;

    public static WeiboContentKafkaProducer getInstance(String table) {
        if (_singleton == null)
            synchronized (WeiboContentKafkaProducer.class) {
                if (_singleton == null) {
                    _singleton = new WeiboContentKafkaProducer(table);
                }
            }
        return _singleton;
    }

    private WeiboContentKafkaProducer(String table) {
        super(table, Tables.table(Tables.KFK_PK_WB_TP));
    }
}
