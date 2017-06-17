package com.datastory.banyan.retry;

import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.hbase.hooks.HBasePutRetryLogHook;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.io.DataSinkWriter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.retry.HBaseRetryWriter
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class HBaseRetryWriter implements RetryWriter, DataSinkWriter {
    static Logger LOG = Logger.getLogger(HBaseRetryWriter.class);
    private String table;
    private RFieldPutter putter;
    private List<DataSinkWriteHook> hooks = new LinkedList<>();

    public HBaseRetryWriter(String table) {
        this.table = table;
        this.putter = new RFieldPutter(table);
        this.putter.getHooks().clear();
        this.putter.getHooks().add(new HBasePutRetryLogHook("hbase." + table));
    }

    public void setEsWriterHook(ESWriterAPI esWriter, Class<? extends ParamsDocMapper> klass) {
        this.putter.setEsWriterHook(esWriter, klass);
    }

    @Override
    public void write(String record) throws Exception {
        if (!BanyanTypeUtil.valid(record)) return;
        Params p = GsonSerializer.deserialize(record, Params.class);
        if (BanyanTypeUtil.valid(p))
            this.putter.batchWrite(p);
    }

    @Override
    public void flush() throws Exception {
        this.putter.flush();
    }

    @Override
    public void close() {
    }


    @Override
    public List<DataSinkWriteHook> getHooks() {
        return hooks;
    }
}
