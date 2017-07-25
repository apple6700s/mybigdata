package com.datastory.banyan.test.kafka.producer;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.kafka.ToESKafkaProtocol;
import com.datastory.commons.kafka.producer.KafkaProducer;
import com.datastory.commons.kafka.utils.DateUtils;
import com.datastory.commons.kafka.utils.HBaseUtils;
import com.datastory.commons.kafka.utils.HTableInterfacePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.test.kafka.producer.ScanHBasePKProducer
 *
 * @author lhfcws
 * @since 16/11/28
 */
@Deprecated
public class ScanHBasePKProducer extends KafkaProducer {
    private String table;
    private int limit = 10000;

    public ScanHBasePKProducer(String topic) {
        super(topic, RhinoETLConfig.getInstance().get(RhinoETLConsts.KAFKA_BROKER_LIST));
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String getKafkaBrokerList() {
        return RhinoETLConfig.getInstance().get(RhinoETLConsts.KAFKA_BROKER_LIST);
    }

    public void run() throws IOException {
        Scan scan = HBaseUtils.buildScan();
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        ResultScanner scanner = hTableInterface.getScanner(scan);
        AtomicInteger cnt = new AtomicInteger(0);
        while (true) {
            if (cnt.get() > limit) break;
            cnt.incrementAndGet();
            Result result = scanner.next();
            if (result == null) break;
            String pk = Bytes.toString(result.getRow());

            ToESKafkaProtocol toESKafkaProtocol = new ToESKafkaProtocol();
            toESKafkaProtocol.setTable(getTable());
            toESKafkaProtocol.setPk(pk);
            toESKafkaProtocol.setPublish_date(DateUtils.getCurrentTimeStr());
            toESKafkaProtocol.setUpdate_date(DateUtils.getCurrentTimeStr());

            String msg = toESKafkaProtocol.toJson();

            batchSend(pk + System.currentTimeMillis(), msg);
        }
        System.out.println("FLUSH " + cnt.get());
        if (cnt.get() > 0
                )
            flushBatchSend();
    }

    public static void main(String[] args) throws IOException {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        String topic = args[0];
        String table = args[1];

        ScanHBasePKProducer pkProducer = new ScanHBasePKProducer(topic);
        pkProducer.setTable(table);
        if (args.length >= 3) {
            pkProducer.setLimit(Integer.parseInt(args[2]));
        }

        pkProducer.run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
