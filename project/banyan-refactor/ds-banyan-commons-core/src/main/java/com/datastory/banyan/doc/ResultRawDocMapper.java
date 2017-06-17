package com.datastory.banyan.doc;

import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * com.datatub.rhino.doc.ResultRDocMapper
 * Result with "r" family -> Params
 * @author lhfcws
 * @since 2016/11/9
 */
public class ResultRawDocMapper extends DocMapper {
    protected static final byte[] RAW_FAMILY = "raw".getBytes();
    protected Result result;
    protected Params doc;

    public ResultRawDocMapper(Result result) {
        this.result = result;
    }


    @Override
    public String getString(String key) {
        return HBaseUtils.getValue(result, RAW_FAMILY, key.getBytes());
    }

    @Override
    public Integer getInt(String key) {
        return BanyanTypeUtil.parseIntForce(getString(key));
    }

    @Override
    public Params map() {
        this.doc = new Params();

        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, RAW_FAMILY);
        fmap.remove("_0");
        fmap.put("pk", Bytes.toString(result.getRow()));

        this.doc.putAll(fmap);
        return this.doc;
    }
}
