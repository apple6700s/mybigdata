package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.hbase.HBaseUtils;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.hbase.WbUserHBaseReader
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WbUserHBaseReader extends HBaseReader {
    private static volatile WbUserHBaseReader _singleton = null;

    public static WbUserHBaseReader getInstance() {
        if (_singleton == null)
            synchronized (WbUserHBaseReader.class) {
                if (_singleton == null) {
                    _singleton = new WbUserHBaseReader();
                }
            }
        return _singleton;
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_WBUSER_TBL);
    }

    @Override
    public Get makeGet(String pk) {
        Get get = super.makeGet(pk);
        get.addFamily("f".getBytes());
        return get;
    }

    @Override
    public Params result2Params(Result result) {
        Params p = new ResultRDocMapper(result).map();
        Map<String, String> mp = HBaseUtils.getFamilyMap(result, "f".getBytes());
        if (mp != null && !mp.isEmpty()) {
            List<String> fans = new LinkedList<>();
            for (String fansUID : mp.keySet()) {
                fans.add(fansUID);
            }
            p.put("follow_list", fans);
        }
        return p;
    }
}
