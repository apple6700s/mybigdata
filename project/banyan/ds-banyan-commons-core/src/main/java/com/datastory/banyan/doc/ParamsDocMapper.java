package com.datastory.banyan.doc;

import com.datastory.banyan.utils.BanyanTypeUtil;
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
        setIn(in);
    }

    public ParamsDocMapper(Map<String, ? extends Object> mp) {
        this(new Params(mp));
    }

    public ParamsDocMapper setIn(Params in) {
        this.in = new Params(in);
        return this;
    }

    public ParamsDocMapper add(Map<String, ? extends Object> p) {
        this.in.putAll(p);
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

    public Short getShort(String key) {
        return BanyanTypeUtil.parseShortForce(getString(key));
    }

    public long getLong(String key) {
        Long l = BanyanTypeUtil.parseLong(in.getString(key));
        if (l == null)
            l = 0l;
        return l;
    }
}
