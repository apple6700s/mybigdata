package com.dt.mig.sync.hbase.doc;

import com.google.common.base.Function;

import java.io.Serializable;

/**
 * com.datatub.rhino.doc.DocMapper
 *
 * @author lhfcws
 * @since 2016/10/17
 */
public abstract class DocMapper implements Serializable {
    public abstract String getString(String key);

    public abstract Integer getInt(String key);

    public abstract Object map();

    /**
     * 单纯的闭包而已
     *
     * @param func
     * @return
     */
    public DocMapper function(Function<Void, Void> func) {
        try {
            func.apply(null);
        } catch (Exception ignore) {
        }
        return this;
    }

}
