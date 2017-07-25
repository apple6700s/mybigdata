package com.datastory.banyan.weibo.kafka;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.kafka.PKSpoutKafkaProducer;
import com.datastory.banyan.utils.Args;

import java.util.HashMap;
import java.util.List;

import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * com.datastory.banyan.weibo.kafka.WeiboUserKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public class WeiboUserKafkaProducer extends PKSpoutKafkaProducer {

    private static volatile WeiboUserKafkaProducer _singleton = null;

    public static WeiboUserKafkaProducer getInstance(String table) {
        if (_singleton == null)
            synchronized (WeiboUserKafkaProducer.class) {
                if (_singleton == null) {
                    _singleton = new WeiboUserKafkaProducer(table);
                }
            }
        return _singleton;
    }

    private WeiboUserKafkaProducer(String table) {
        super(table, Tables.table(Tables.KFK_PK_WB_TP));
    }

    @Override
    public synchronized void afterWrite(Object writeRequest, Object writeResponse) {
        List<Args> argsList = (List<Args>) writeRequest;
        int[] res = (int[]) writeResponse;
        int successCnt = 0;
        // discard duplicate users in a batch
        HashMap<String, Args> mp = new HashMap<>();
        for (int i = 0; i < res.length; i++) {
            int r = res[i];
            if (r >= 0 || r == SUCCESS_NO_INFO) {
                successCnt++;
                Args args = argsList.get(i);
                String pk = String.valueOf(args.get(0));
                mp.put(pk, args);
            }
        }

        for (Args args : mp.values()) {
            _batchSend(args);
        }
        mp.clear();
        System.out.println("[KAFKA] Sending " + successCnt + " ToESKafkaProtocol to " + topic);
        flushBatchSend();
    }
}
