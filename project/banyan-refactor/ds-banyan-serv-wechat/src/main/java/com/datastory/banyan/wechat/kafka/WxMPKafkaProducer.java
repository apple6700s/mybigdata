package com.datastory.banyan.wechat.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.PKSpoutKafkaProducer;

/**
 * com.datastory.banyan.wechat.WxMPKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WxMPKafkaProducer extends PKSpoutKafkaProducer {

    private static volatile WxMPKafkaProducer _singleton = null;

    public static WxMPKafkaProducer getInstance() {
        if (_singleton == null)
            synchronized (WxMPKafkaProducer.class) {
                if (_singleton == null) {
                    _singleton = new WxMPKafkaProducer();
                }
            }
        return _singleton;
    }

    private WxMPKafkaProducer() {
        super(Tables.table(Tables.PH_WXMP_TBL), Tables.table(Tables.KFK_PK_WX_TP));
    }
}
