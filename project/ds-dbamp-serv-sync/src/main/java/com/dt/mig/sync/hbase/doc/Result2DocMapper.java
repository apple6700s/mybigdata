package com.dt.mig.sync.hbase.doc;


import com.dt.mig.sync.utils.HBaseUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * com.datatub.rhino.doc.ResultRDocMapper
 * Result with "r" family -> Params
 *
 * @author lhfcws
 * @since 2016/11/9
 */
public class Result2DocMapper extends DocMapper {
    private byte[] family = "r".getBytes();
    private Result result;
    private Params doc;

    public Result2DocMapper(Result result) {
        this.result = result;
    }


    public Result2DocMapper(Result result, byte[] family) {
        this.result = result;
        this.family = family;
    }


    @Override
    public String getString(String key) {
        return HBaseUtil.getValue(result, family, key.getBytes());
    }

    @Override
    public Integer getInt(String key) {
        try {
            return Integer.parseInt(key);
        } catch (Exception ignore) {
            return 0;
        }
    }

    public String getPk() {
        return Bytes.toString(result.getRow());
    }


    @Override
    public Params map() {
        Map<String, String> fmap = HBaseUtil.getFamilyMap(result, family);
        if (fmap != null) {
            this.doc = new Params(fmap);
            this.doc.put("pk", Bytes.toString(result.getRow()));
            this.doc.remove("_0");
        }
        return this.doc;
    }
}
