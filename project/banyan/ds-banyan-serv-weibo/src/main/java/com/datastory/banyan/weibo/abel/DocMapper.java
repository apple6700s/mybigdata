package com.datastory.banyan.weibo.abel;

import com.google.common.base.Function;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * com.datatub.rhino.doc.DocMapper
 *
 * @author lhfcws
 * @since 2016/10/17
 */
public abstract class DocMapper implements Serializable {
    protected static Logger LOG = Logger.getLogger(DocMapper.class);

    public DocMapper clear() {
        return this;
    }

    public abstract String getString(String key);

    public abstract Integer getInt(String key);

    /**
     * 单纯的闭包而已
     * @param func
     * @return
     */
    public DocMapper function(Function<Void, Void> func) {
        try {
            func.apply(null);
        } catch (Exception ignore) {}
        return this;
    }

    public abstract Object map();
}
