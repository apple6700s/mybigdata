package com.datastory.banyan.es;

import com.datastory.banyan.async.Async;
import com.datastory.banyan.async.AsyncPool;
import com.datastory.banyan.async.AsyncRet;
import com.datastory.banyan.async.Function;
import com.datastory.banyan.es.hooks.*;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.impl.BulkResAck;
import com.datastory.banyan.utils.Condition;
import com.datastory.banyan.utils.ShutdownHookManger;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.es.BanyanEsBulkClient
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class BanyanEsBulkClient extends SimpleEsBulkClient {
    private static Logger LOG = Logger.getLogger(BanyanEsBulkClient.class);
    protected List<Request<ActionRequest>> buffer = new ArrayList<Request<ActionRequest>>();
    protected AtomicInteger flushCnt = new AtomicInteger(0);

    public BanyanEsBulkClient(String clusterName, String indexName, String indexType, String[] hosts, int bulkActions) {
        super(clusterName, indexName, indexType, hosts, bulkActions);
        String shutdownName = "[" + this.getClass().getSimpleName() + "] " + getBulkName();
        ShutdownHookManger.removeShutdownHook(shutdownName);
        ShutdownHookManger.addShutdownHook(shutdownName, new Runnable() {
            @Override
            public void run() {
                try {
                    flush(true);
                    close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public DataSinkWriteHook getMainHook() {
        return new EsMain2Hook(
                new EsMonitor2Hook(getBulkName()),
                new EsRetry2Hook(this),
                new EsRetryLog2Hook(getBulkName())
        );
    }

    public void add(Request<ActionRequest> req) {
        if (isSyncMode()) {
            try {
                new Condition() {
                    @Override
                    public boolean satisfy() {
                        return !(isSyncMode() && flushing.get());
                    }
                }.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        synchronized (this) {
            buffer.add(req);
        }

        if (buffer.size() > bulkActions) {
            flush();
        }
    }

    @Override
    public void add(ActionRequest req) {
        add(new Request<ActionRequest>(req));
    }

    @Override
    public void flush(final boolean await) {
        if (!buffer.isEmpty()) {
            if (isSyncMode()) {
                flushing.compareAndSet(false, true);
            }

            try {
                final RequestList<ActionRequest> requestList;
                synchronized (this) {
                    if (!buffer.isEmpty()) {
                        requestList = new RequestList<>(buffer);
                        buffer.clear();
                    } else {
                        return;
                    }
                }

                // before hooks
                for (DataSinkWriteHook hook : getHooks()) {
                    hook.beforeWrite(requestList);
                }

                // build BulkRequest
                BulkRequest bulkRequest = new BulkRequest();
                for (Request<ActionRequest> req : requestList) {
                    bulkRequest.add(req.getRequestObj());
                }

                if (status.get() < 0)
                    requestList.disableRetry();

                // execute & async after hooks
                final CountDownLatch latch;
                if (await)
                    latch = new CountDownLatch(1);
                else
                    latch = null;

                reconnectClientIfNeeded();

                flushCnt.addAndGet(1);
                clientLock.readLock().lock();
                try {
                    client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(final BulkResponse res) {
                            Async.async(AsyncPool.MONITOR_POOL, new Function() {
                                @Override
                                public AsyncRet call() throws Exception {
                                    Ack ack = new BulkResAck(res);
                                    for (DataSinkWriteHook hook : getHooks()) {
                                        hook.afterWrite(requestList, ack);
                                    }
                                    flushCnt.addAndGet(-1);
                                    if (await)
                                        latch.countDown();
                                    return null;
                                }
                            });
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            Async.async(AsyncPool.MONITOR_POOL, new Function() {
                                @Override
                                public AsyncRet call() throws Exception {
                                    for (DataSinkWriteHook hook : getHooks()) {
                                        hook.afterWrite(requestList, e);
                                    }
                                    flushCnt.addAndGet(-1);
                                    if (await)
                                        latch.countDown();
                                    return null;
                                }
                            });
                        }
                    });
                    if (await && !getHooks().isEmpty())
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                } finally {
                    clientLock.readLock().unlock();
                }
            } finally {
                if (isSyncMode()) {
                    flushing.compareAndSet(true, false);
                }
            }
        }
    }

    @Override
    public int getCurrentSize() {
        synchronized (this) {
            return buffer.size();
        }
    }

    @Override
    public void close() throws InterruptedException {
        status.compareAndSet(0, -1);
        int f;
        while ((f = flushCnt.get()) > 0) {
            LOG.info("[Remain batches to flush] " + f);
            Thread.sleep(5000);
        }

        super.close();
    }
}
