package com.dt.mig.sync.hbase.doc;

import com.yeezhao.commons.util.Entity.Params;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by abel.chan on 16/12/9.
 */
public abstract class DocWrapper<T> {

    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    protected static final DateTimeFormatter BIRTH_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    protected Params params;

    public DocWrapper(Params params) {
        this.params = params;
    }

    public abstract T objWrapper();
}
