package com.datastory.banyan.migrate1.flush;

import java.util.HashMap;

/**
 * com.datastory.banyan.migrate1.flush.FlushParams
 *
 * @author lhfcws
 * @since 2017/6/28
 */
public class FlushParams extends HashMap<String, FlushParam> {
    public FlushParams add(FlushParam fp) {
        put(fp.getHbTable(), fp);
        return this;
    }
}
