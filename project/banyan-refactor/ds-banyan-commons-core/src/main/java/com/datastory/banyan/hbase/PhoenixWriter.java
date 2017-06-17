package com.datastory.banyan.hbase;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.doc.ParamsDocMapper;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.hooks.*;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.io.DataSinkWriter;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.impl.JDBCAck;
import com.datastory.banyan.utils.*;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Reference;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;


/**
 * com.datastory.banyan.hbase.PhoenixWriter
 * 约定 pk, update_date, publish_date/create_date 为Fields 前三位，方便spout
 *
 * @author lhfcws
 * @since 16/11/22
 */

public abstract class PhoenixWriter implements Serializable, DataSinkWriter {
    public static final String ENABLE_HOOK = "banyan.es.hooks.enable";
    protected static Logger LOG = Logger.getLogger(PhoenixWriter.class);
    protected static final String GENERIC_UPSERT_SQL = "UPSERT INTO \"%s\" (%s) VALUES (%s)";
    protected String sql = null;
    protected PhoenixDriver driver;
    protected int cacheSize = 1000;
    protected List<Request<Args>> buffer = new ArrayList<Request<Args>>();
    protected List<String> fields = new LinkedList<>();
    protected List<DataSinkWriteHook> hooks = new LinkedList<>();
    // es
    protected FactoryFunc<? extends ESWriterAPI> esWriterFactory = null;
    protected ESWriterAPI esWriter = null;
    protected Class<? extends ParamsDocMapper> docMapperClass = null;


    public PhoenixWriter() {
        initPhoenixDriver();
        if (!isLazyInit()) {
            final Class<? extends PhoenixWriter> klass = this.getClass();
            ShutdownHookManger.addShutdownHook(klass, new Runnable() {
                @Override
                public void run() {
                    System.out.println("[SHUTDOWN HOOK] invoke " + klass);
                    flush(true);
                    System.out.println("[SHUTDOWN HOOK] done " + klass);
                }
            });
            if (RhinoETLConfig.getInstance().getBoolean(ENABLE_HOOK, true)) {
                getHooks().add(
                        new PhoenixMain2Hook(
                                new PhoenixMonitor2Hook(getTable()),
                                new PhoenixRetry2Hook(this),
                                new PhoenixRetryLog2Hook(this)
                        )
                );
            }
            init();
        }

    }

    public PhoenixWriter(int cacheSize) {
        this();
        this.cacheSize = cacheSize;
    }

    protected boolean isLazyInit() {
        return false;
    }

    protected void init() {
        while (true) {
            try {
                setFields(this.driver.getAllColumnNames(getTable()));
                break;
            } catch (SQLException e) {
                ErrorUtil.error(LOG, e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public PhoenixDriver getDriver() {
        return driver;
    }

    public abstract String getTable();

    public boolean isIgnoreExistsRow() {
        return false;
    }

    public String getSql() {
        return sql;
    }

    protected void initPhoenixDriver() {
        driver = PhoenixDriverFactory.getDriver();
    }

    public void setFields(List<String> fields) {
        this.fields.clear();
        this.fields.add("pk");
        if (fields.contains("update_date"))
            this.fields.add("update_date");
        if (fields.contains("publish_date"))
            this.fields.add("publish_date");
        else if (fields.contains("create_date"))
            this.fields.add("create_date");

        Set<String> tmpSet = new HashSet<>(this.fields);
        for (String field : fields) {
            if (!tmpSet.contains(field))
                this.fields.add(field);
        }
        String fieldsStr = BanyanTypeUtil.joinNWrap(this.fields, ",", "\"", "\"");
        String symbols = BanyanTypeUtil.repeat("?", fields.size(), ",");
        sql = String.format(GENERIC_UPSERT_SQL, getTable(), fieldsStr, symbols)
                + (isIgnoreExistsRow() ? " ON DUPLICATE KEY IGNORE" : "");
    }

    public void setFields(String[] fields) {
        setFields(Arrays.asList(fields));
    }

    public int[] batchWrite(Request<Args> req) {
        synchronized (this) {
            buffer.add(req);
        }

        if (buffer.size() >= cacheSize) {
            synchronized (this) {
                if (buffer.size() >= cacheSize) {
                    RequestList<Args> requestList = new RequestList<>(buffer);
                    buffer.clear();
                    return flush(requestList, false);
                }
            }
        }
        return null;
    }

    public int[] batchWrite(Args args) {
        return batchWrite(new Request<Args>(args));
    }

    public int[] batchWrite(List<String> values) {
        if (CollectionUtil.isEmpty(values))
            return null;
        String[] arr = new String[values.size()];
        arr = values.toArray(arr);
        return batchWrite(arr);
    }

    public int[] batchWrite(Map<String, ? extends Object> map) {
        if (map == null || map.isEmpty()) return null;

        String[] arr = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Object v = map.get(fields.get(i));
            if (v != null)
                arr[i] = String.valueOf(v);
            else
                arr[i] = null;
        }
        return batchWrite(arr);
    }

    public int[] batchWrite(String... values) {
        Args args = new RetryableArgs(values);
        return batchWrite(args);
    }

    public int[] flush() {
       return flush(false);
    }

    public int[] flush(boolean await) {
        if (!buffer.isEmpty()) {
            RequestList<Args> requestList;
            synchronized (this) {
                if (!buffer.isEmpty()) {
                    requestList = new RequestList<>(buffer);
                    buffer.clear();
                } else
                    return null;
            }
            return flush(requestList, await);
        } else
            return null;
    }

    public int[] flush(final RequestList<Args> requestList, final boolean await) {
        final Reference<int[]> resRef = new Reference<>(null);
        List<Args> argses = null;
        if (requestList.size() > 0) {
            try {
                for (final DataSinkWriteHook hook : getHooks()) {
                    Async.async(AsyncPool.MONITOR_POOL, new com.datastory.banyan.async.Function() {
                        @Override
                        public AsyncRet call() throws Exception {
                            hook.beforeWrite(requestList);
                            return null;
                        }
                    });
                }

                if (await)
                    requestList.disableRetry();

                argses = requestList.toRequestObjs();
                List<Object[]> sqlParams = Args.toObjectArrays(argses);

                int[] res = driver.batchExecute(getSql(), sqlParams);

                try {
                    esWriterHook(argses, res);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                resRef.setValue(res);
                return res;
            } catch (Exception e) {
                LOG.error(BanyanTypeUtil.prettyStringifyList(argses));
                LOG.error(e.getMessage(), e);

                return null;
            } finally {
                if (resRef.getValue() == null)
                    resRef.setValue(new int[0]);
                final CountDownLatch latch = new CountDownLatch(1);
                Async.async(AsyncPool.MONITOR_POOL, new com.datastory.banyan.async.Function() {
                    @Override
                    public AsyncRet call() throws Exception {
                        for (final DataSinkWriteHook hook : getHooks()) {
                            hook.afterWrite(requestList, new JDBCAck(resRef.getValue()));
                        }
                        latch.countDown();
                        return null;
                    }
                });
                if (await)
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            }
        } else
            return null;
    }

    @Override
    public List<DataSinkWriteHook> getHooks() {
        return hooks;
    }

    public PhoenixWriter setEsWriterHook(ESWriterAPI esWriter, Class<? extends ParamsDocMapper> klass) {
        this.esWriter = esWriter;
//        if (this.esWriter instanceof ESWriter) {
//            ((ESWriter) this.esWriter).setEnableIdleClose(true);
//        }
        this.docMapperClass = klass;
        return this;
    }

    public PhoenixWriter setEsWriterHook(FactoryFunc<? extends ESWriterAPI> esWriterFactory, Class<? extends ParamsDocMapper> klass) {
        this.esWriterFactory = esWriterFactory;
        this.docMapperClass = klass;
        return this;
    }

    public FactoryFunc<? extends ESWriterAPI> getEsWriterFactory() {
        return esWriterFactory;
    }

    public ESWriterAPI getEsWriter() {
        return esWriter;
    }

    public void esWriterHook(List<Args> clist, int[] res) {
        if (esWriter == null && esWriterFactory == null)
            return;

        ESWriterAPI writer = esWriter;
        if (writer == null) {
            writer = esWriterFactory.create();
        }

        try {
            ParamsDocMapper dm = null;
            if (docMapperClass != null)
                dm = ClassUtil.newInstance(docMapperClass, new Params());

            try {
                int i = 0;
                for (int r : res) {
                    if (Status.isJDBCSuccess(r)) {
                        Args args = clist.get(i);
                        Params hbDoc = args.link(fields);
                        if (dm != null) {
                            dm.setIn(hbDoc);
                            Params esDoc = (Params) dm.map();
                            if (esDoc != null)
                                writer.write(esDoc);
                        } else {
                            writer.write(hbDoc);
                        }
                    }
                    i++;
                }
            } catch (Exception e) {
                LOG.error(e);
            } finally {
                if (esWriterFactory != null) {
                    writer.flush();
                    writer.close();
                    writer = null;
                } else
                    writer.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
