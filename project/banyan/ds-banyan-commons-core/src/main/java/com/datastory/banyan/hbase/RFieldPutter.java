package com.datastory.banyan.hbase;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.async.Function;
import com.datastory.banyan.doc.BanyanPutDocMapper;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.io.DataSinkWriter;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.impl.PutAck;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.ShutdownHookManger;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Reference;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 单独对某些字段进行put
 * <p>
 * com.datastory.banyan.hbase.RFieldPutter
 *
 * @author lhfcws
 * @since 16/11/15
 */

public class RFieldPutter implements Serializable, DataSinkWriter {
    protected static Logger LOG = Logger.getLogger(RFieldPutter.class);

    public static final int DEFAULT_RETRY = 3;
    protected List<Request<Put>> buffer = new LinkedList<>();
    protected String table;
    protected byte[] family = "r".getBytes();
    protected List<DataSinkWriteHook> hooks = new ArrayList<>();
    protected int cacheSize = 1000;
    protected ESWriterAPI esWriter = null;
    protected Class<? extends ParamsDocMapper> docMapperClass = null;

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public RFieldPutter(String table) {
        this.table = table;
        final Class<? extends RFieldPutter> klass = this.getClass();
        ShutdownHookManger.addShutdownHook("[" + this.getClass().getSimpleName() + "] " + getTable(), new Runnable() {
            @Override
            public void run() {
                try {
                    flush(true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public String getTable() {
        return table;
    }

    public int batchWriteByTimeField(Map<String, ? extends Object> p, String timeField) throws Exception {
        String d = (String) p.get(timeField);
        if (!BanyanTypeUtil.valid(d))
            return -1;
        Date date = DateUtils.parse(d, DateUtils.DFT_TIMEFORMAT);
        long ts = date.getTime();
        return batchWrite(p, ts);
    }

    public int batchWrite(Map<String, ? extends Object> p) throws IOException {
        String pk = (String) p.remove("pk");
        if (StringUtil.isNullOrEmpty(pk))
            throw new IOException("pk should not be null");

        Put put = new Put(pk.getBytes());
        for (Map.Entry<String, ? extends Object> e : p.entrySet()) {
            if (e.getValue() == null) continue;
            String v = String.valueOf(e.getValue());
            put.addColumn(getFamily(), e.getKey().getBytes(), v.getBytes());
        }
        return batchWrite(put);
    }

    public int batchWrite(Map<String, ? extends Object> p, long timestamp) throws IOException {
        String pk = (String) p.remove("pk");
        if (StringUtil.isNullOrEmpty(pk))
            throw new IOException("pk should not be null");

        Put put = new Put(pk.getBytes(), timestamp);
        for (Map.Entry<String, ? extends Object> e : p.entrySet()) {
            if (e.getValue() == null) continue;
            String v = String.valueOf(e.getValue());
            put.addColumn(getFamily(), e.getKey().getBytes(), v.getBytes());
        }
        return batchWrite(put);
    }

    public int batchWrite(Put put) throws IOException {
        return batchWrite(new Request<Put>(put));
    }

    public int batchWrite(Request<Put> req) throws IOException {
        synchronized (buffer) {
            buffer.add(req);
        }
        if (buffer.size() > getCacheSize()) {
            synchronized (this) {
                if (buffer.size() > getCacheSize())
                    return flush();
            }
        }
        return 0;
    }

    public RFieldPutter setEsWriterHook(ESWriterAPI esWriter, Class<? extends ParamsDocMapper> klass) {
        this.esWriter = esWriter;
//        if (this.esWriter instanceof ESWriter) {
//            ((ESWriter) this.esWriter).setEnableIdleClose(true);
//        }
        this.docMapperClass = klass;
        return this;
    }

    public int flush() throws IOException {
        return flush(false);
    }

    public int flush(final boolean await) throws IOException {
        final RequestList<Put> requestList;
        synchronized (buffer) {
            requestList = new RequestList<>(buffer);
            buffer.clear();
        }
        final Reference<Integer> ref = new Reference<>(0);
        if (requestList.size() > 0) {
            Connection conn = null;
            Table hti = null;
            try {
                conn = Connections.get();
                hti = Connections.getTable(conn, table);
                for (DataSinkWriteHook hook : getHooks()) {
                    hook.beforeWrite(requestList);
                }

                if (await)
                    requestList.disableRetry();

                List<Put> clist = requestList.toRequestObjs();
                hti.put(clist);

                esWriterHook(clist, clist.size());
                ref.setValue(clist.size());
                return ref.getValue();
            } catch (Exception e) {
                ref.setValue(-1);
                LOG.error(e.getMessage(), e);
                return ref.getValue();
            } finally {
                Connections.close(conn, hti);
                final CountDownLatch latch = new CountDownLatch(1);
                Async.async(AsyncPool.MONITOR_POOL, new Function() {
                    @Override
                    public AsyncRet call() throws Exception {
                        PutAck ack = new PutAck(ref.getValue());
                        for (DataSinkWriteHook hook : getHooks()) {
                            hook.afterWrite(requestList, ack);
                        }
                        latch.countDown();
                        return null;
                    }
                });
                if (await && !getHooks().isEmpty())
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            }
        } else
            return ref.getValue();
    }

    @Override
    public List<DataSinkWriteHook> getHooks() {
        return hooks;
    }

    public ESWriterAPI getEsWriter() {
        return esWriter;
    }

    public void esWriterHook(List<Put> clist, int res) {
        if (clist == null || clist.size() == 0 || esWriter == null || !Status.isHBaseSuccess(res))
            return;

        ESWriterAPI writer = esWriter;
        try {
            ParamsDocMapper dm = null;
            if (docMapperClass != null)
                dm = ClassUtil.newInstance(docMapperClass, new Params());

            for (Put put : clist) {
                Params hbDoc = new BanyanPutDocMapper(put).map();
                if (dm != null) {
                    dm.setIn(hbDoc);
                    Params esDoc = (Params) dm.map();
                    if (esDoc != null)
                        writer.write(esDoc);
                } else {
                    writer.write(hbDoc);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
        } finally {
            writer.flush();
        }
    }
}
