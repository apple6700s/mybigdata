package com.dt.mig.sync.es;

import com.dt.mig.sync.words.IWords;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Created by abel.chan on 16/12/7.
 */
public class CommonQueryBuilder {

    public static QueryBuilder buildQueryBuilder(String field, String... ids) {
        BoolQueryBuilder boolFilter = QueryBuilders.boolQuery();

        boolFilter.must(QueryBuilders.termsQuery(field, ids));

        return boolFilter;
    }

    public static BoolQueryBuilder buildQueryBuilder(List<String> keyFields, List<String> filterFields, IWords keyWords, IWords filterWords, List<String> filterDocIds, String timeField, long start, long end) {

        BoolQueryBuilder boolFilter = QueryBuilders.boolQuery();
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery(timeField).from(start).to(end);
        boolFilter.filter(rangeQuery);

        if (keyWords != null) {
            for (List<String[]> titleToKeyWords : keyWords.getWords().values()) {
                if (titleToKeyWords == null || titleToKeyWords.size() <= 0) continue;
                for (String[] key : titleToKeyWords) {
                    if (key == null || key.length <= 0) continue;
                    boolFilter.should(buildMutiMatchPhraseQuery(keyFields, key));
                }
            }
            boolFilter.minimumNumberShouldMatch(1);
        }

        if (filterWords != null) {
            for (List<String[]> titleToFilterWords : filterWords.getWords().values()) {
                if (titleToFilterWords == null || titleToFilterWords.size() <= 0) continue;
                for (String[] filter : titleToFilterWords) {
                    if (filter == null || filter.length <= 0) continue;
                    boolFilter.mustNot(buildMutiMatchPhraseQuery(filterFields, filter));
                }
            }
        }

        if (filterDocIds != null && filterDocIds.size() > 0) {
            boolFilter.mustNot(QueryBuilders.termsQuery("id", filterDocIds));
        }

        return boolFilter;
    }

    private static QueryBuilder buildMutiMatchPhraseQuery(List<String> fields, String[] keywords) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String field : fields) {
            boolQuery.should(buildMustMatchPhraseQuery(field, keywords));
        }
        boolQuery.minimumNumberShouldMatch(1);
        return boolQuery;
    }

    private static QueryBuilder buildMustMatchPhraseQuery(String field, String[] keywords) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String keyword : keywords) {
            boolQuery.must(QueryBuilders.matchPhraseQuery(field, keyword).slop(0));
        }
        return boolQuery;
    }

}
