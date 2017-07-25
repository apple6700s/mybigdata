package com.dt.mig.sync.es;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * Created by abel.chan on 17/1/27.
 */
public class CommonAggBuilder {

    //默认的size大小
    private final static int DEFAULT_AGG_SIZE = 10;

    public static AggregationBuilder buildTermsAgg(String field) throws Exception {
        return buildTermsAgg(field, DEFAULT_AGG_SIZE);
    }

    public static AggregationBuilder buildTermsAgg(String field, int size) throws Exception {
        if (StringUtils.isEmpty(field)) {
            throw new Exception("field is not null!");
        }
        AggregationBuilder builder = AggregationBuilders.terms(field).size(size).field(field);
        return builder;
    }

    public static AggregationBuilder buildTermsAgg(String field, int size, boolean isAsc) throws Exception {
        if (StringUtils.isEmpty(field)) {
            throw new Exception("field is not null!");
        }
        AggregationBuilder builder = AggregationBuilders.terms(field).order(Terms.Order.term(isAsc)).size(size).field(field);
        return builder;
    }

}
