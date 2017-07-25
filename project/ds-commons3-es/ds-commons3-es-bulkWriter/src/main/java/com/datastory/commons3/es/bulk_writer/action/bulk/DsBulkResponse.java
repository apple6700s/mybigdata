package com.datastory.commons3.es.bulk_writer.action.bulk;

import org.elasticsearch.action.bulk.BulkItemResponse;

public class DsBulkResponse {

    /**
     * 总共花费的时间，毫秒
     */
    private long tookInMillis;
    /**
     * 总共进行的bulk调用次数
     */
    private int numOfBulks;

    /**
     * 每个子ActionRequest的Response
     */
    private BulkItemResponse[] responses;
    /**
     * 每个子Response的状态
     */
    private BulkItemResponseState[] states;
    /**
     * 总的bulk调用时间
     */
    private long bulkInMillis;
    /**
     * 是否全部子Response都是成功的
     */
    private boolean allResponsesSuccess;

    /**
     * 在没有一次返回BulkResponse时的最后一次异常
     */
    private Throwable nonResponseCause;

    public DsBulkResponse() {
        this.tookInMillis = 0;
        this.numOfBulks = 0;

        this.responses = new BulkItemResponse[0];
        this.states = new BulkItemResponseState[0];
        this.bulkInMillis = 0;
        this.allResponsesSuccess = true;

        this.nonResponseCause = null;
    }

    public DsBulkResponse(long tookInMillis, int numOfBulks, Throwable nonResponseCause) {
        this.tookInMillis = tookInMillis;
        this.numOfBulks = numOfBulks;
        this.nonResponseCause = nonResponseCause;

        this.responses = new BulkItemResponse[0];
        this.states = new BulkItemResponseState[0];
        this.bulkInMillis = 0;
        this.allResponsesSuccess = true;
    }

    public DsBulkResponse(long tookInMillis, int numOfBulks, BulkItemResponse[] responses, BulkItemResponseState[] states, long bulkInMillis, boolean allResponsesSuccess) {
        this.tookInMillis = tookInMillis;
        this.numOfBulks = numOfBulks;
        this.responses = responses;
        this.states = states;
        this.bulkInMillis = bulkInMillis;
        this.allResponsesSuccess = allResponsesSuccess;

        this.nonResponseCause = null;
    }

    public BulkItemResponse[] getResponses() {
        return responses;
    }

    public BulkItemResponseState[] getStates() {
        return states;
    }

    public boolean isAllResponsesSuccess() {
        return allResponsesSuccess;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public long getBulkInMillis() {
        return bulkInMillis;
    }

    public int getNumOfBulks() {
        return numOfBulks;
    }

    public boolean hasResponseAtLeastOnce() {
        return nonResponseCause == null;
    }

    public Throwable getNonResponseCause() {
        return nonResponseCause;
    }
}
