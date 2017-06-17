package com.datastory.banyan.doc;

import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * com.datatub.rhino.doc.ResultRDocMapper
 * Result with "r" family -> Params
 * @author lhfcws
 * @since 2016/11/9
 */
public class ResultRDocMapper extends DocMapper {
    private static final byte[] R_FAMILY = "r".getBytes();
    private Result result;
    private Params doc;

    public ResultRDocMapper(Result result) {
        this.result = result;
    }


    @Override
    public String getString(String key) {
        return HBaseUtils.getValue(result, R_FAMILY, key.getBytes());
    }

    @Override
    public Integer getInt(String key) {
        return BanyanTypeUtil.parseIntForce(getString(key));
    }

    @Override
    public Params map() {
        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, R_FAMILY);
        this.doc = new Params(fmap);
        this.doc.put("pk", Bytes.toString(result.getRow()));
        this.doc.remove("_0");
        return this.doc;
    }

    public StrParams mapStr() {
        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, R_FAMILY);
        StrParams ret = new StrParams(fmap);
        ret.put("pk", Bytes.toString(result.getRow()));
        ret.remove("_0");
        return ret;
    }
}
