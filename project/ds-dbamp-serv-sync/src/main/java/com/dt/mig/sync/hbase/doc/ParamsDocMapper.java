package com.dt.mig.sync.hbase.doc;

import com.dt.mig.sync.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.Map;

/**
 * com.datatub.banyan.doc.ParamsDocMapper
 *
 * @author lhfcws
 * @since 2016/10/25
 */
public abstract class ParamsDocMapper extends DocMapper {
    protected Params in;

    public ParamsDocMapper(Params in) {
        this.in = in;
    }

    public ParamsDocMapper(Map<String, ? extends Object> mp) {
        this(new Params(mp));
    }

    public ParamsDocMapper setIn(Params in) {
        this.in = in;
        return this;
    }

    @Override
    public String getString(String key) {
        return in.getString(key);
    }

    @Override
    public Integer getInt(String key) {
        return BanyanTypeUtil.parseIntForce(in.getString(key));
    }

    public long getLong(String key) {
        Long l = BanyanTypeUtil.parseLong(in.getString(key));
        if (l == null) l = 0l;
        return l;
    }
}
