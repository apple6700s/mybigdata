package com.datastory.commons3.es.bulk_writer.action.bulk;


import com.datastory.commons3.es.bulk_writer.annotation.UnThreadSafe;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@UnThreadSafe
public class DsBulkProcessor {

    private final static Logger LOG = Logger.getLogger(DsBulkProcessor.class);

    static class BulkRequestFactory {

        TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;
        WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;
        boolean refresh = false;

        public BulkRequest bulkRequest() {
            return Requests.bulkRequest()
                    .timeout(timeout)
                    .refresh(refresh)
                    .consistencyLevel(consistencyLevel);
        }
    }

    public static class Builder {

        protected AbstractClient client;

        private BulkRequestFactory bulkRequestFactory;
        private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
        private boolean ignoreAlreadyExistsException = true;

        public Builder(AbstractClient client) {
            this.client = client;
            this.bulkRequestFactory = createBulkRequestFactory();
        }

        public Builder setTimeout(TimeValue timeout) {
            bulkRequestFactory.timeout = timeout;
            return this;
        }

        public Builder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
            bulkRequestFactory.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder setRefresh(boolean refresh) {
            bulkRequestFactory.refresh = refresh;
            return this;
        }

        public Builder setBackoffPolicy(BackoffPolicy backoffPolicy) {
            this.backoffPolicy = backoffPolicy;
            return this;
        }

        public void setIgnoreAlreadyExistsException(boolean ignoreAlreadyExistsException) {
            this.ignoreAlreadyExistsException = ignoreAlreadyExistsException;
        }

        protected BulkRequestFactory createBulkRequestFactory() {
            return new BulkRequestFactory();
        }

        public DsBulkProcessor build() {
            return new DsBulkProcessor(client, bulkRequestFactory, backoffPolicy, ignoreAlreadyExistsException);
        }
    }

    private final AbstractClient client;
    private final BulkRequestFactory bulkRequestFactory;
    private final BackoffPolicy backoffPolicy;
    private final boolean ignoreAlreadyExistsException;

    private BulkRequest bulkRequest;

    private DsBulkProcessor(AbstractClient client,
                            BulkRequestFactory bulkRequestFactory,
                            BackoffPolicy backoffPolicy,
                            boolean ignoreAlreadyExistsException) {
        this.client = client;
        this.bulkRequestFactory = bulkRequestFactory;
        this.backoffPolicy = backoffPolicy;
        this.ignoreAlreadyExistsException = ignoreAlreadyExistsException;
    }

    /**
     * @throws ActionRequestValidationException
     */
    public DsBulkProcessor addWithValidate(ActionRequest request) throws ActionRequestValidationException {
        ActionRequestValidationException ex = request.validate();
        if (ex != null) {
            throw ex;
        }
        if (bulkRequest == null) {
            bulkRequest = bulkRequestFactory.bulkRequest();
        }
        bulkRequest.add(request);
        return this;
    }

    public DsBulkProcessor add(ActionRequest request) {
        if (bulkRequest == null) {
            bulkRequest = bulkRequestFactory.bulkRequest();
        }
        bulkRequest.add(request);
        return this;
    }

    public DsBulkResponse flush() throws IllegalArgumentException, InterruptedException {
        if (bulkRequest == null || bulkRequest.numberOfActions() == 0) {
            return new DsBulkResponse();
        }

        BulkRequest bulkRequest = this.bulkRequest;
        List<Integer> indexes = null;
        this.bulkRequest = null;

        BulkItemResponse[] responses = new BulkItemResponse[bulkRequest.numberOfActions()];
        BulkItemResponseState[] states = new BulkItemResponseState[bulkRequest.numberOfActions()];
        long bulkInMillis = 0;
        int numOfBulks = 0;
        boolean allResponsesSuccess = true;

        long startTime = System.currentTimeMillis();
        Iterator<TimeValue> iterator = backoffPolicy.iterator();

        Throwable nonResponseCause = null;
        for (;;) {
            numOfBulks++;
            boolean hasNext = iterator.hasNext();

            BulkResponse response = null;
            try {
                ActionFuture<BulkResponse> future = client.bulk(bulkRequest);
                response = future.actionGet();
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Throwable t) {
                if (t instanceof IllegalStateException && "Future got interrupted".equals(t.getMessage())) {
                    Throwable t1 = t.getCause();
                    if (t1 instanceof InterruptedException) {
                        throw (InterruptedException) t1;
                    }
                }
                LOG.warn("bulk failed, then retry", nonResponseCause = t);
            }

            if (response != null) {
                BulkRequest _bulkRequest = bulkRequestFactory.bulkRequest();
                List<Integer> _indexes = new ArrayList<>();

                bulkInMillis += response.getTookInMillis();
                BulkItemResponse[] itemResponses = response.getItems();
                for (int i = 0; i < itemResponses.length; i++) {
                    BulkItemResponse itemResponse = itemResponses[i];
                    BulkItemResponseState state = BulkItemResponseState.getState(itemResponse.getFailure());

                    int index = indexes != null ? indexes.get(i) : i;
                    responses[index] = itemResponse;
                    states[index] = state;

                    switch (state) {
                        case ALREADY_EXISTS:
                            if (!ignoreAlreadyExistsException) {
                                allResponsesSuccess = false;
                            }
                            break;
                        case DATA_WRONG:
                            allResponsesSuccess = false;
                            break;
                        case REQUEST_FAIL:
                            if (hasNext) {
                                _bulkRequest.add(bulkRequest.requests().get(i));
                                _indexes.add(index);
                            } else {
                                allResponsesSuccess = false;
                            }
                            break;
                    }
                }
                if (_indexes.isEmpty()) {
                    break;
                }
                bulkRequest = _bulkRequest;
                indexes = _indexes;
            }

            if (hasNext) {
                TimeValue timeValue = iterator.next();
                Thread.sleep(timeValue.millis());
            } else {
                break;
            }
        }

        long tookInMillis = System.currentTimeMillis() - startTime;
        if (states[0] != null) {
            return new DsBulkResponse(tookInMillis, numOfBulks,
                    responses, states, bulkInMillis, allResponsesSuccess);
        } else {
            return new DsBulkResponse(tookInMillis, numOfBulks,
                    nonResponseCause);
        }
    }
}
