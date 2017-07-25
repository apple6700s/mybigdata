package com.datastory.banyan.retry;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.hooks.ESRetryLogHook;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * com.datastory.banyan.retry.ESRetryWriter
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class ESRetryWriter implements RetryWriter {
    private static Logger LOG = Logger.getLogger(ESRetryWriter.class);
    private String indexName;
    private String indexType;
    private ESWriter esWriter;

    public ESRetryWriter(String indexName, String indexType) {
        this.indexName = indexName;
        this.indexType = indexType;

    }

    public void init(ESWriter ew) {
        if (ew != null)
            this.esWriter = ew;
        else {
            esWriter = new ESWriter(indexName, indexType, 4000);
            esWriter.getHooks().clear();
            esWriter.getHooks().add(new ESRetryLogHook("es." + indexName + "." + indexType));
        }
    }

    @Override
    public void write(String record) throws Exception {
        if (!BanyanTypeUtil.valid(record)) return;
        Params doc = null;
        doc = GsonSerializer.deserialize(record, Params.class);
        if (BanyanTypeUtil.valid(doc)) {

            esWriter.write(doc);
        }
    }

    @Override
    public void flush() {
        esWriter.flush();
    }

    @Override
    public void close() {
        try {
            if (esWriter != null)
                esWriter.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
