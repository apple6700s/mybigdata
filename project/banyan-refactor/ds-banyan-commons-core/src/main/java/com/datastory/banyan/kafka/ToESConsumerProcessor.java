package com.datastory.banyan.kafka;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseReader;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * com.datastory.banyan.kafka.ToESConsumerProcessor
 *
 * @author lhfcws
 * @since 16/11/24
 */

public abstract class ToESConsumerProcessor implements Serializable {
    protected static Logger LOG = Logger.getLogger(ToESConsumerProcessor.class);

    public abstract HBaseReader getHBaseReader();

    public abstract Params mapDoc(Params p);

    public abstract ESWriterAPI getESWriter();

    public boolean canProcess(ToESKafkaProtocol protocol) {
        return getHBaseReader().getTable().equals(protocol.getTable());
    }

    public void process(ToESKafkaProtocol protocol) throws Exception {
        if (canProcess(protocol)) {
            String pk = protocol.getPk();
            List<Params> reads = getHBaseReader().batchRead(pk);
            if (CollectionUtil.isNotEmpty(reads)) {
                writeEs(reads);
            }
        }
    }

    public void flush() throws Exception {
        List<Params> reads = null;
        synchronized (getHBaseReader().getClass()) {
            reads = getHBaseReader().flush();
        }

        if (CollectionUtil.isNotEmpty(reads)) {
            writeEs(reads);
        }
    }

    protected void writeEs(List<Params> reads) {
        ESWriterAPI esWriter = getESWriter();
        for (Params p : reads) {
            if (p == null || p.size() <= 1)
                continue;
            Params esDoc = mapDoc(p);
            try {
                esWriter.write(esDoc);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
//        if (reads.size() > 0)
//            esWriter.flush();
    }
}
