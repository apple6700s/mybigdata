package com.datastory.banyan.wechat.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.PKSpoutKafkaProducer;

/**
 * com.datastory.banyan.wechat.WxContentKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class WxContentKafkaProducer extends PKSpoutKafkaProducer {

    private static volatile WxContentKafkaProducer _singleton = null;

    public static WxContentKafkaProducer getInstance() {
        if (_singleton == null)
            synchronized (WxContentKafkaProducer.class) {
                if (_singleton == null) {
                    _singleton = new WxContentKafkaProducer();
                }
            }
        return _singleton;
    }

    private WxContentKafkaProducer() {
        super(Tables.table(Tables.PH_WXCNT_TBL), Tables.table(Tables.KFK_PK_WX_TP));
    }
}
