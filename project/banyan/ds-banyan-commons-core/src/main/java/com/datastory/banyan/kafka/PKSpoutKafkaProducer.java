package com.datastory.banyan.kafka;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.utils.Args;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.commons.kafka.producer.KafkaProducer;

import java.util.List;

import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * com.datastory.banyan.kafka.PKSpoutKafkaProducer
 *
 * @author lhfcws
 * @since 16/11/24
 */
@Deprecated
public abstract class PKSpoutKafkaProducer extends KafkaProducer implements DataSinkWriteHook {
    protected String table;
    public PKSpoutKafkaProducer(String table, String topic) {
        super(topic, RhinoETLConfig.getInstance().get(RhinoETLConsts.KAFKA_BROKER_LIST));
        this.table = table;
    }

    @Override
    public void beforeWrite(Object writeRequest) {
        // pass
    }

    @Override
    public synchronized void afterWrite(Object writeRequest, Object writeResponse) {
        List<Args> argsList = (List<Args>) writeRequest;
        int[] res = (int[]) writeResponse;
        int successCnt = 0;
        for (int i = 0; i < res.length; i++) {
            int r = res[i];
            if (r >= 0 || r == SUCCESS_NO_INFO) {
                successCnt++;
                Args args = argsList.get(i);
                _batchSend(args);
            }
        }
        System.out.println("[KAFKA] Sending " + successCnt + " ToESKafkaProtocol to " + topic);
        flushBatchSend();
    }

    protected void _batchSend(Args args) {
        String pk = String.valueOf(args.get(0));
        String updateDate = String.valueOf(args.get(1));
        if (!DateUtils.validateDatetime(updateDate))
            updateDate = DateUtils.getCurrentTimeStr();
        String publishDate = String.valueOf(args.get(2));
        if (!DateUtils.validateDatetime(publishDate))
            publishDate = updateDate;

        ToESKafkaProtocol protocol = new ToESKafkaProtocol(pk, table, updateDate, publishDate);
        batchSend(pk, protocol.toJson());
    }
}
