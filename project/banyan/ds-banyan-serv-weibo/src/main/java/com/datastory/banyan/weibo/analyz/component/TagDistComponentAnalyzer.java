package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.RFieldGetter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.TagDistAnalyzer;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * com.datastory.banyan.weibo.analyz.component.TagDistComponentAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public class TagDistComponentAnalyzer implements ComponentAnalyzer {
    public static TagDistAnalyzer tagDistAnalyzer = new TagDistAnalyzer();
    public static final String[] FANS_RANGE = {"1000", "500000"}; // 讲道理，这个范围应该是算法组提供接口的。
    public static final byte[] R = "r".getBytes();
    public static final byte[] UID = "uid".getBytes();
    public static final byte[] TAG_DIST = "tag_dist".getBytes();
    public static final byte[] TAGS = "tags".getBytes();
    public static final byte[] FANS_CNT = "fans_cnt".getBytes();
    public static FilterList filterList;

    static {
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.GREATER_OR_EQUAL, FANS_RANGE[0].getBytes());
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.LESS_OR_EQUAL, FANS_RANGE[1].getBytes());
        filterList = new FilterList();
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
    }

    @Override
    public List<String> getInputFields() {
        return Arrays.asList("content");
    }

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        StrParams ret = new StrParams();
        Get get = new Get(BanyanTypeUtil.wbuserPK(uid).getBytes());
        get.addColumn(R, TAGS);
        get.setFilter(filterList);
        RFieldGetter getter = new RFieldGetter(Tables.table(Tables.PH_WBUSER_TBL));
        String tags = null;
        try {
            tags = getter.singleGet(get, "tags");
        } catch (IOException e) {
            return ret;
        }
        if (StringUtil.isNullOrEmpty(tags))
            return ret;

        String tagDist = tagDistAnalyzer.analyz(tags, valueIter);
        if (!StringUtil.isNullOrEmpty(tagDist)) {
            ret.put("tag_dist", tagDist);
        }
        return ret;
    }

    @Override
    public void setup() {
        LOG.error(this.getClass().getSimpleName() + " does not support streaming processing. ");
    }

    @Override
    public void streamingAnalyz(Object obj) {

    }

    @Override
    public StrParams cleanup() {
        return null;
    }
}
