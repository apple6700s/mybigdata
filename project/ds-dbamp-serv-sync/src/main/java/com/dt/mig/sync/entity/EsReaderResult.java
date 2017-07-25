package com.dt.mig.sync.entity;

import org.elasticsearch.search.SearchHit;

/**
 * Created by abel.chan on 16/11/20.
 */
public class EsReaderResult {
    private boolean isEnd;
    private String scrollId;
    private SearchHit[] datas;

    public EsReaderResult(String scrollId, SearchHit[] datas, boolean isEnd) {
        this.scrollId = scrollId;
        this.datas = datas;
        this.isEnd = isEnd;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public SearchHit[] getDatas() {
        return datas;
    }

    public void setDatas(SearchHit[] datas) {
        this.datas = datas;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    public int getDateSize() {
        return datas != null ? datas.length : 0;
    }
}
