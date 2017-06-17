package com.datastory.banyan.weibo.doc;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.doc.DocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.doc.UserFollowListDocMapper
 *
 * @author lhfcws
 * @since 16/12/10
 */

public class UserFollowListDocMapper extends DocMapper {
    public static final String FOLLOW_LIST_KEY = RhinoETLConsts.FOLLOW_LIST_KEY;
    private static final byte[] F = "f".getBytes();
    private Result result;

    public UserFollowListDocMapper(Result result) {
        this.result = result;
    }

    @Override
    public String getString(String key) {
        return null;
    }

    @Override
    public Integer getInt(String key) {
        return null;
    }

    @Override
    public List<String> map() {
        if (result == null || result.isEmpty())
            return null;
        Map<String, String> mp = mapWithRelations();
        if (mp == null || mp.isEmpty()) return null;

        List<String> followList = new ArrayList<>(
                mp.keySet()
        );
        return followList;
    }

    public Map<String, String> mapWithRelations() {
        if (result == null || result.isEmpty())
            return null;
        return HBaseUtils.getFamilyMap(result, F);
    }
}
