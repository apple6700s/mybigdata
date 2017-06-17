package com.datastory.banyan.es.override233;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * com.datastory.banyan.es.override.DsBulkRequestHandler
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public abstract class DsBulkRequestHandler {
    protected final ESLogger logger;
    protected final Client client;

    protected DsBulkRequestHandler(Client client) {
        this.client = client;
        this.logger = Loggers.getLogger(getClass(), client.settings());
    }


    public abstract void execute(BulkRequest bulkRequest, long executionId);

    public abstract boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;


    public static DsBulkRequestHandler syncHandler(Client client, BackoffPolicy backoffPolicy, DsBulkProcessor.Listener listener) {
        return new DsBulkRequestHandler.SyncBulkRequestHandler(client, backoffPolicy, listener);
    }

    public static DsBulkRequestHandler asyncHandler(Client client, BackoffPolicy backoffPolicy, DsBulkProcessor.Listener listener, int concurrentRequests) {
        return new DsBulkRequestHandler.AsyncBulkRequestHandler(client, backoffPolicy, listener, concurrentRequests);
    }

    private static class SyncBulkRequestHandler extends DsBulkRequestHandler {
        private final DsBulkProcessor.Listener listener;
        private final BackoffPolicy backoffPolicy;

        public SyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, DsBulkProcessor.Listener listener) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            this.listener = listener;
        }

        @Override
        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = Retry
                        .on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withSyncBackoff(client, bulkRequest);
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkResponse);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Bulk request {} has been cancelled.", e, executionId);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            } catch (Throwable t) {
                logger.warn("Failed to execute bulk request {}.", t, executionId);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, t);
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            // we are "closed" immediately as there is no request in flight
            return true;
        }
    }

    private static class AsyncBulkRequestHandler extends DsBulkRequestHandler {
        private final BackoffPolicy backoffPolicy;
        private final DsBulkProcessor.Listener listener;
        private final Semaphore semaphore;
        private final int concurrentRequests;

        private AsyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, DsBulkProcessor.Listener listener, int concurrentRequests) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            assert concurrentRequests > 0;
            this.listener = listener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(final BulkRequest bulkRequest, final long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                Retry.on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withAsyncBackoff(client, bulkRequest, new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, response);
                                } finally {
                                    semaphore.release();
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, e);
                                } finally {
                                    semaphore.release();
                                }
                            }
                        });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Bulk request {} has been cancelled.", e, executionId);
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Throwable t) {
                logger.warn("Failed to execute bulk request {}.", t, executionId);
                listener.afterBulk(executionId, bulkRequest, t);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                semaphore.release(this.concurrentRequests);
                return true;
            }
            return false;
        }
    }
}
