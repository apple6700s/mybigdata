package com.datastory.banyan.newsforum.runner;

import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * com.datastory.banyan.newsforum.runner.MResultDocMapper
 *
 * @author zhaozhen
 * @since 2017/7/25
 */
public class MResultDocMapper extends ResultRDocMapper {

    private static final byte[] M_FAMILY = "m".getBytes();
    private Result result;
    private Params doc;

    public MResultDocMapper(Result result) {
        super(result);
    }

    @Override
    public String getString(String key) {
        return HBaseUtils.getValue(result, M_FAMILY, key.getBytes());
    }

    @Override
    public Integer getInt(String key) {
        return BanyanTypeUtil.parseIntForce(getString(key));
    }


    @Override
    public Params map() {
        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, M_FAMILY);
        this.doc = new Params(fmap);
        this.doc.put("pk", Bytes.toString(result.getRow()));
        this.doc.remove("_0");
        return this.doc;
    }


    public StrParams mapStr() {
        Map<String, String> fmap = HBaseUtils.getFamilyMap(result, M_FAMILY);
        StrParams ret = new StrParams(fmap);
        ret.put("pk", Bytes.toString(result.getRow()));
        ret.remove("_0");
        return ret;
    }
}
